
# global imports
from metaflow import FlowSpec, step, S3, Parameter, current, card
from metaflow.cards import Markdown, Table
import lance
import numpy as np
import os
import json
import pyarrow as pa
import shutil
import time
from random import choice

class RecSysSagemakerDeployment(FlowSpec):

    IS_DEV = Parameter(
        name='is_dev',
        help='Flag for dev development, with a smaller dataset',
        default='1'
    )

    KNN_K = Parameter(
        name='knn_k',
        help='Number of neighbors we retrieve from the vector space',
        default='100'
    ) 

    # highlight-start
    # NOTE: Sagemaker-specific parameters below here
    # If you don't wish to deploy the model, you can leave 'sagemaker_deploy' as 0,
    # and ignore the other parameters. Check the README for more details.
    SAGEMAKER_DEPLOY = Parameter(
        name='sagemaker_deploy',
        help='Deploy KNN model with Sagemaker',
        default='0'
    )

    SAGEMAKER_IMAGE = Parameter(
        name='sagemaker_image',
        help='Image to use in the Sagemaker endpoint: this is compatible with our TF recs KNN model',
        default='763104351884.dkr.ecr.us-west-2.amazonaws.com/tensorflow-inference:2.7.0-gpu-py38-cu112-ubuntu20.04-sagemaker'
    )

    SAGEMAKER_INSTANCE = Parameter(
        name='sagemaker_instance',
        help='AWS instance for the Sagemaker endpoint: this may be expensive!',
        default='ml.p3.2xlarge'
    )

    SAGEMAKER_ROLE = Parameter(
        name='sagemaker_role',
        help='IAM role in AWS to use to spin up the Sagemaker endpoint',
        default='MetaSageMakerRole'
    )
    # highlight-end

    # NOTE to save disk space, the dataset now is *NOT* versioned with
    # Metaflow - you can uncomment this and modify the function below if 
    # you prefer to track the dataset file in the data store 
    # from metaflow import IncludeFile
    # playlist_dataset = IncludeFile(
    #    'playlist_dataset',
    #    help='CC Dataset from Kaggle',
    #    default='cleaned_spotify_dataset.parquet')

    @step
    def start(self):
        """
        Start-up: check everything works or fail fast!
        """
        from metaflow.metaflow_config import DATASTORE_SYSROOT_S3 
        # debug printing
        print("flow name: %s" % current.flow_name)
        print("run id: %s" % current.run_id)
        print("username: %s" % current.username)
        # if you're using Metaflow with AWS, you should see an s3 url here!
        print("datastore is: %s" % DATASTORE_SYSROOT_S3)
        if self.IS_DEV == '1':
            print("ATTENTION: RUNNING AS DEV VERSION - DATA WILL BE SUB-SAMPLED!!!") 
        # highlight-start
        if self.SAGEMAKER_DEPLOY == '1':
            print("ATTENTION: DEPLOYMENT TO SAGEMAKER IS ENABLED!") 
            # if we deploy to Sagemaker, the model needs to versioned in S3 first
            # through Metaflow s3 datastore - here we check that the data store
            # is configured properly
            assert DATASTORE_SYSROOT_S3 is not None
        # highlight-end
        # next up, get the data
        self.next(self.prepare_dataset)

    @step
    def prepare_dataset(self):
        """
        Get the data in the right shape by reading the parquet dataset
        and using duckdb SQL-based wrangling to quickly prepare the datasets for
        training our Recommender System.
        """
        import duckdb
        import numpy as np
        # we start a fast in-memory database
        con = duckdb.connect(database=':memory:')
        # read the data from the local dataset file
        # if you prefer to rely on Metaflow versioning for this input file
        # uncomment the IncludeFile at the top of the class and modify the
        # reading line
        # note we create a new id for the playlist, by concatenating user and playlist name
        # since songs can have the same name (e.g. Intro), we make them (more?) unique by
        # concatenating the artist and the track with a special symbol |||
        con.execute("""
            CREATE TABLE playlists AS 
            SELECT *, 
            CONCAT (user_id, '-', playlist) as playlist_id,
            CONCAT (artist, '|||', track) as track_id,
            FROM 'cleaned_spotify_dataset.parquet'
            ;
        """)
        # quick inspection of the first line
        con.execute("SELECT * FROM playlists LIMIT 1;")
        print(con.fetchone())
        # let's leverage duckdb super fast SQL interface to get some descriptive stats
        # about our dataset. Some people may prefer to do as part of EDA in a notebook,
        # but we include it here since i) it's super fast, ii) it shows a viable alternative
        # to clunky Pandas code for simple aggregations
        tables = ['row_id', 'user_id', 'track_id', 'playlist_id', 'artist']
        for t in tables:
            con.execute("SELECT COUNT(DISTINCT({})) FROM playlists;".format(t))
            print("# of {}".format(t), con.fetchone()[0])
        # when we are happy, we are getting data in the shape we need for Prod2Vec type 
        # of representation - each row is keyed by playlist id, and with two arrays
        # for the sequence of artists in the playlist and the sequence of songs
        # 9cc0cfd4d7d7885102480dd99e7a90d6-HardRock | [ artist_1, ... artist_n ] | [ song_1, ... song_n ] 
        # We use the original row_id as index for the playlist ordering
        # NOTE: we create an intermediate table so that we can optionally subsample
        # at the end and get a smaller dataset
        sampling_cmd = ''
        if self.IS_DEV == '1':
            # data will be sampled down if this is a dev run
            # this allows for very quick iteration and small data
            # snapshots as we still work our way towards an end to end flow
            print("Subsampling data, since this is DEV")
            sampling_cmd = ' USING SAMPLE 10 PERCENT (bernoulli)'
        # build the dataset query
        # NOTE: we also sequenci-fy the artist as a list in case 
        # we want to build artist embeddings ;-)
        # track_test_x is the list of songs in a playlist except the LAST one
        # track_test_y is the LAST song - we will use these columns for 
        # validation and testing of our recommender, when asking the model to
        # "continue" a playlist it has never seen before.
        dataset_query = """
            SELECT * FROM
            (   
                SELECT 
                    playlist_id,
                    LIST(artist ORDER BY row_id ASC) as artist_sequence,
                    LIST(track_id ORDER BY row_id ASC) as track_sequence,
                    array_pop_back(LIST(track_id ORDER BY row_id ASC)) as track_test_x,
                    LIST(track_id ORDER BY row_id ASC)[-1] as track_test_y
                FROM 
                    playlists
                GROUP BY playlist_id 
                HAVING len(track_sequence) > 2
            ) 
            {}
            ;
            """.format(sampling_cmd)
        # dump the table to a df and print out stats
        con.execute(dataset_query)
        df = con.fetch_df()
        print("# rows: {}".format(len(df)))
        # debug: print the first row
        print(df.iloc[0].tolist())
        # close out the db connection
        con.close()
        # assign session to training, validation and test set
        train, validate, test = np.split(
            df.sample(frac=1, random_state=42), 
            [int(.7 * len(df)), int(.9 * len(df))])
        # finally, version the data in Metaflow automatically
        # since pandas can be pickled by default
        self.df_dataset = df
        self.df_train = train
        self.df_validate = validate
        self.df_test = test
        print("# testing rows: {}".format(len(self.df_test)))
        # next up, generate vectors for songs from existing playlists
        # sets of hypers - we serialize them to a string and pass them to the foreach below
        # params inspired by https://arxiv.org/pdf/2007.14906.pdf
        self.hypers_sets = [json.dumps(_) for _ in [
            { 'min_count': 3, 'epochs': 30, 'vector_size': 48, 'window': 10, 'ns_exponent': 0.75 },
            { 'min_count': 5, 'epochs': 30, 'vector_size': 48, 'window': 10, 'ns_exponent': 0.75 },
            { 'min_count': 10, 'epochs': 30, 'vector_size': 48, 'window': 10, 'ns_exponent': 0.75 }
        ]]
        # we train K models in parallel, depending how many configurations of hypers 
        # we set - we generate K set of vectors, and evaluate them on the validation
        # set to pick the best combination of parameters!
        self.next(self.generate_embeddings, foreach='hypers_sets')

    def predict_next_track(self, vector_space, input_sequence, k):
        """        
        Given an embedding space, predict best next song with KNN.
        Initially, we just take the LAST item in the input playlist as the query item for KNN
        and retrieve the top K nearest vectors (you could think of taking the smoothed average embedding
        of the input list, for example, as a refinement).

        If the query item is not in the vector space, we make a random bet. We could refine this by taking
        for example the vector of the artist (average of all songs), or with some other strategy (sampling
        by popularity). 

        For more options on how to generate vectors for "cold items" see for example the paper:
        https://dl.acm.org/doi/10.1145/3383313.3411477
        """
        query_item = input_sequence[-1]
        if query_item not in vector_space:
            # pick a random item instead
            query_item = choice(list(vector_space.index_to_key))
        
        return [_[0] for _ in vector_space.most_similar(query_item, topn=k)]

    def evaluate_model(self, _df, vector_space, k):
        lambda_predict = lambda row: self.predict_next_track(vector_space, row['track_test_x'], k)
        _df['predictions'] = _df.apply(lambda_predict, axis=1)
        lambda_hit = lambda row: 1 if row['track_test_y'] in row['predictions'] else 0
        _df['hit'] = _df.apply(lambda_hit, axis=1)
        # debug: print the first row where we got a hit
        # this is a pretty long print, so uncomment only when debugging
        # print(_df[_df['hit'] == 1].iloc[0].tolist())
        # hit rate is # of hits / total predictions
        hit_rate = _df['hit'].sum() / len(_df)
        return hit_rate

    @step
    def generate_embeddings(self):
        """
        Generate vector representations for songs, based on the Prod2Vec idea.

        For an overview of the algorithm and the evaluation, see for example:
        https://arxiv.org/abs/2007.14906
        """
        from gensim.models.word2vec import Word2Vec
        # this is the CURRENT hyper param JSON in the fan-out
        # each copy of this step in the parallelization will have its own value
        self.hyper_string = self.input
        self.hypers = json.loads(self.hyper_string)
        track2vec_model = Word2Vec(self.df_train['track_sequence'], **self.hypers)
        print("Training with hypers {} is completed!".format(self.hyper_string))
        print("Vector space size: {}".format(len(track2vec_model.wv.index_to_key)))
        # debug with a random example
        test_track = choice(list(track2vec_model.wv.index_to_key))
        print("Example track: '{}'".format(test_track))
        test_vector = track2vec_model.wv[test_track]
        print("Test vector for '{}': {}".format(test_track, test_vector[:5]))
        test_sims = track2vec_model.wv.most_similar(test_track, topn=3)
        print("Similar songs to '{}': {}".format(test_track, test_sims))
        # calculate the validation score as hit rate
        self.validation_metric = self.evaluate_model(
            self.df_validate,
            track2vec_model.wv,
            k=int(self.KNN_K))
        print("Hit Rate@{} is: {}".format(self.KNN_K, self.validation_metric))
        # finally, version the embeddings
        self.track_vectors = track2vec_model.wv
        # join with the other runs
        self.next(self.join_runs)

    def write_vectors(self, vector_space):
        emb_type = pa.list_(pa.float32(), list_size=48)
        schema = pa.schema([pa.field("track", pa.string(), False),
                            pa.field("embedding", emb_type, False)])
        vectors = vector_space.vectors / np.sqrt((vector_space.vectors ** 2).sum(axis=1))[:, None]
        tbl = pa.Table.from_arrays([pa.array(vector_space.index_to_key),
                                    pa.FixedSizeListArray.from_arrays(
                                        pa.array(vectors.ravel(), type=pa.float32()),
                                        list_size=48)
                                    ],
                                   schema=schema)
        uri = "embeddings.lance"
        shutil.rmtree(uri)
        lance.write_dataset(tbl, uri)
        return lance.dataset(uri)

    @card(type='blank', id='hyperCard')
    @step
    def join_runs(self, inputs):
        """
        Join the parallel runs and merge results into a dictionary.
        """
        # merge results from runs with different parameters (key is hyper settings as a string)
        # and collect the predictions made by the different versions
        self.all_vectors = { inp.hyper_string: inp.track_vectors for inp in inputs}
        self.all_results = { inp.hyper_string: inp.validation_metric for inp in inputs}
        print("Current result map: {}".format(self.all_results))
         # pick one according to best hit rate
        self.best_model, self_best_result = sorted(self.all_results.items(), key=lambda x: x[1], reverse=True)[0]
        print("The best validation score is for model: {}, {}".format(self.best_model, self_best_result))
        # assign as "final" the best vectors according to validation
        self.final_vectors = self.all_vectors[self.best_model]
        self.final_dataset = inputs[0].df_test
        # TODO: improve card
        current.card.append(Markdown("## Results from parallel training"))
        current.card.append(
            Table([
                [inp.hyper_string, inp.validation_metric] for inp in inputs
            ])
        )
        # write vectors to disk
        self.write_vectors(self.final_vectors)
        # next, test the best model on unseen data, and report the final Hit Rate as 
        # our best point-wise estimate of "in the wild" performance
        self.next(self.model_testing)

    @step
    def model_testing(self):
        """
        Test the generalization abilities of the best model by running predictions
        on the unseen test data.

        We report a quantitative point-wise metric, hit rate @ K, as an initial implementation. However,
        evaluating recommender systems is a very complex task, and better metrics, through good abstractions, 
        are available, i.e. https://reclist.io/.
        """
        self.test_metric = self.evaluate_model(
            self.final_dataset,
            self.final_vectors,
            k=int(self.KNN_K))
        print("Hit Rate@{} on the test set is: {}".format(self.KNN_K, self.test_metric))
        self.next(self.deploy)

    # highlight-next-line
    def build_retrieval_model(self):
        """
        Take the embedding space, build a Keras KNN model and store it in S3
        so that it can be deployed by a Sagemaker endpoint!
        
        While for simplicity this function is embedded in the deploy step,
        you could think of spinning it out as it's own step.
        """
        dataset = lance.dataset("embeddings.lance")
        df = dataset.to_table().to_pandas()
        vectable = zip(*[s.values() for s in list(df.to_dict().values())])
        lookup = {track: embedding for track, embedding in vectable}

        def find_topk(track, k=10):
            q = lookup[track]
            return dataset.to_table(nearest={
                "column": "embedding", "q": q, "k": k
            })
        return find_topk


    # highlight-start
    @step
    def deploy(self):
    # highlight-end
        """
        Inspired by: https://github.com/jacopotagliabue/no-ops-machine-learning/blob/main/flow/training.py
        
        Use SageMaker to deploy the model as a stand-alone, PaaS endpoint, with our choice of the underlying
        Docker image and hardware capabilities.

        Available images for inferences can be chosen from AWS official list:
        https://github.com/aws/deep-learning-containers/blob/master/available_images.md
        
        """
        import numpy as np
        self.all_ids = list(self.final_vectors.index_to_key)
        self.startup_embeddings = np.array([self.final_vectors[_] for _ in self.all_ids])
        # skip the deployment if not needed
        if self.SAGEMAKER_DEPLOY == '0':
            print("Skipping deployment to Sagemaker")
        else:
            predictor = self.build_retrieval_model()
            # run a small test against the endpoint to check everything is working fine
            input = self.all_ids[self.test_index]
            # output is on the form {'predictions': {'output_2': ['0012E00001z5EzAQAU', ..]}
            import time
            start = time.time()
            result = predictor(input)
            end = time.time()
            print(input, result, end - start)

        self.next(self.end)

    @step
    def end(self):
        """
        Just say bye!
        """
        print("All done\n\nSee you, space cowboy\n")
        return


if __name__ == '__main__':
    RecSysSagemakerDeployment()
