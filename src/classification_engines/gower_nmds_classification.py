import numpy as np
import pandas as pd
import gower
from scipy.cluster.hierarchy import linkage, fcluster, dendrogram, cut_tree
from scipy.spatial.distance import squareform

from sklearn.manifold import MDS

class GowerNMDS():

    weights_ = None
    raw_matrix_ = None
    features_vectors_ = None

    def setup(self, df, weights):
        self.weights_ = weights
        self.raw_matrix_ = gower.gower_matrix(df, weight=self.weights_)

    def matrix_to_dataframe(self):
        return pd.DataFrame(self.raw_matrix_)

    def calculate_linkage(self):
        Zd = linkage(squareform(self.raw_matrix_, force='tovector'), 'complete')
        clusters = fcluster(Zd, 2, criterion='maxclust')
        return Zd, clusters

    def _2d_representation(self):
        #dimensionality reduction like pca
        mds = MDS(n_components=2, dissimilarity='precomputed', metric=False)
        mds_result = mds.fit_transform(self.raw_matrix_)
        xs = mds_result[:,0]
        ys = mds_result[:,1]
        scaled_x = xs * 1.0/(xs.max() - xs.min())
        scaled_y = ys * 1.0/(ys.max() - ys.min())
        self.features_vectors_ = np.transpose(mds_result[:2])
        return scaled_x, scaled_y, self.features_vectors_

