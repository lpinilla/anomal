from data_drivers.StaticDriver import InputDriver, OutputDriver
import pandas as pd
from elasticsearch import Elasticsearch, helpers

class ElasticOutputDriver(OutputDriver):

    client = None
    index = None

    def connect(self):
        self.client = Elasticsearch(http_compress=True)
        return True

    def doc_generator(self, df):
        df.fillna(0) #convert NaN to 0
        df_iter = df.iterrows()
        for index, doc in df_iter:
            yield {
                    "_index": self.index,
                    "_type": "_doc",
                    "_source": {key: doc[key] for key in list(df.columns)}
            }

    def save(self, df):
        helpers.bulk(self.client, self.doc_generator(df))

    def disconnect(self):
        self.client.close()
        return True

