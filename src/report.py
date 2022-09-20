import pandas as pd
import numpy as np
import pickle
import yaml

from scipy.cluster.hierarchy import linkage, fcluster, dendrogram, cut_tree
from scipy.spatial.distance import pdist,squareform
from collections import Counter
from tabulate import tabulate
from functools import partial

from bokeh.io import curdoc, output_notebook, show
from bokeh.layouts import column, row
from bokeh.models import ColumnDataSource, Select, Slider, HoverTool
from bokeh.palettes import *
from bokeh.plotting import figure, output_file, ColumnDataSource
from bokeh.embed import file_html, server_document, components

import plotly.figure_factory as ff
import plotly.io as pio
import plotly.graph_objects as go
import plotly.express as px

from pywebio import start_server, config
from pywebio.output import *
from pywebio.pin import put_slider, put_select, pin, pin_wait_change
from pywebio.session import download

class Report():

    df = None
    margins = {'b':10, 't':5, 'l':0, 'r':1}
    n_clusters = 2
    graph_size = 350
    features_importance = None
    styles = {'high': 'color:red', 'medium': 'color:orange', 'low': 'color:yellow'}
    df_original = None
    weights = None
    flags = None
    Zd = None
    clusters = None
    gow = None
    scaled_x = None #MDS data
    scaled_y = None #MDS data
    dend_plot = None #baked dendrogram plot
    corr_plot = None #baked correlation plot
    features_vectors = None #baked features vectors
    only_flags = None

    md_intro = None
    md_features = None

    def __init__(self, df, df_original, classification_engine, flags):
        self.df = df
        self.df_original = df_original
        self.flags = flags
        if classification_engine is not None:
            self.only_flags = False
            self.weights = classification_engine.weights_
            self.gow = classification_engine.raw_matrix_
            self.Zd, self.clusters = classification_engine.calculate_linkage()
            self.df['label'] = self.clusters
            self.scaled_x, self.scaled_y, self.features_vectors = classification_engine._2d_representation()
            self.features_vectors = classification_engine.features_vectors_
            self.features_importance = self.df.corr(numeric_only=True)['label'].drop(['label']).abs().sort_values(ascending=True).reset_index().rename(columns={'index': 'variable', 'label':'contribution'})
            self.md_intro= '''
    # Anomal results

    The presented results are given by running the system agains a dataset of %d records, taking into account %d features.
    ''' % (len(df), len(df.columns) -1)
            self.md_features = '''
    ## Features Info

    The used features were **%s**.

    The features that most influenced the labeling where **%s**.
        ''' % ('**, **'.join(self.df.columns), ', '.join(self.features_importance['variable'][:3]))
        else:
            self.only_flags = True


    def get_suspects(self):
        #Always grab the last group which is the one with the fewest members
        suspects_id = self.df.iloc[np.where(self.clusters == list(Counter(self.clusters))[-1])[0]].index
        #add flagged ids
        for flag in self.flags:
            suspects_id = np.append(suspects_id, np.array(flag['result'].index))
        return self.df_original.loc[suspects_id].sort_index(), self.df.loc[suspects_id].sort_index()

    def bkapp(self, doc):
        max_clusters = 7
        spectral = Paired[8]
        colors = [spectral[i] for i in self.clusters]
        clusters_slider = Slider(title='Number of Clusters', value=2, start=2, end=max_clusters, step=1, width=150)
        testdf = self.df.copy()
        testdf['x'] = self.scaled_x
        testdf['y'] = self.scaled_y
        testdf['colors'] = colors
        source = ColumnDataSource(testdf)
        #callback function to update the values
        def update_mds_result_colors(attrname, old, new):
            self.n_clusters = int(clusters_slider.value)
            self.clusters = fcluster(self.Zd, self.n_clusters, criterion='maxclust')
            colors = [spectral[i] for i in self.clusters]
            source.data = dict(colors=colors, x=self.scaled_x, y=self.scaled_y)
        #slider
        clusters_slider.on_change('value', update_mds_result_colors)
        #On Mouse Hover show label with data id
        tooltips = [
            ( 'id', '@id'),
            ( 'client.bytes', '@{client.bytes}'),
            ( 'network.bytes', '@{network.bytes}'),
            ( 'numbers_in_hostname', '@{numbers_in_hostname}'),
            ( 'hostname_entropy', '@{hostname_entropy}')
        ]
        #The plot itself
        mds_result_plot= figure(toolbar_location=None, title='MDS', width=self.graph_size, height=self.graph_size, tooltips=tooltips)
        mds_result_plot.circle('x','y', fill_color='colors',line_color=None,source=source)
        #Labels
        #for i in range(len(self.features_vectors)):
        #    mds_result_plot.text(x='x',y='y', text='text', color='color', source=ColumnDataSource(pd.DataFrame.from_records([dict(x=self.features_vectors[i][0],y=self.features_vectors[i][1], text=self.features_importance['variable'][i], color='white')])))
        #    mds_result_plot.segment(x0='x0',y0='y0', x1='x1', y1='y1', line_color='white', source=ColumnDataSource(pd.DataFrame.from_records([dict(x0=0, y0=0, x1=self.features_vectors[i][0],y1=self.features_vectors[i][1])])))
        doc.theme = 'dark_minimal'
        doc.add_root(column([mds_result_plot, clusters_slider]))
        doc.title = "PF"

    def build_dendogram(self):
        pio.templates.default = "plotly_dark"
        fig = ff.create_dendrogram(self.Zd, truncate_mode='lastp', show_leaf_counts=False, leaf_rotation=0, show_contracted=True, leaf_font_size=0)
        fig.update_layout(width=self.graph_size, height=self.graph_size, margin=self.margins, paper_bgcolor="#191d21", plot_bgcolor="#24292e")
        self.dend_plot = fig.to_html(include_plotlyjs="require", full_html=True)

    def feature_relevance_graph(self):
        fig = go.Figure(go.Bar(
                x=[round(i, 2) for i in self.features_importance['contribution']],
                y=self.features_importance['variable'],
                orientation='h'))
        fig.update_layout(autosize=False, width=400, height=350, margin=self.margins)
        return fig.to_html(include_plotlyjs="require", full_html=False)

    def suspects_features_group(self):
        suspects_id = np.where(self.clusters == list(Counter(self.clusters))[-1])
        return self.df.iloc[suspects_id].drop(['label'], axis=1).to_html(border=0)

    #select input to select feature
    def build_distplot_by_feature(self, feature):
        Xs = []
        self.n_clusters = len(Counter(self.clusters))
        group_with_one_element = False
        for x in range(self.n_clusters):
            df_cluster = self.df[self.df['label']==(x+1)][feature].fillna(0)
            if len(df_cluster) == 1 :
                group_with_one_element = True
                break
            #Filtering empty values (different than Nan)
            Xs.append([x for x in df_cluster if x != ''])
        if group_with_one_element:
            #Filter empty values (different than NaN)
            fig = px.histogram(self.df[self.df[feature] != ''], x=feature, color='label')
        else:
            groups = ["Group%d"%i for i in range(self.n_clusters)]
            try:
                fig = ff.create_distplot(Xs, groups, show_rug=False)
            except np.linalg.LinAlgError:
                fig = px.histogram(self.df[self.df[feature] != ''], x=feature, color='label')
        fig.update_layout(autosize=False, width=400, height=350, margin=self.margins)
        return fig.to_html(include_plotlyjs="require", full_html=False)

    def build_corr_plot(self):
        fig = px.imshow(self.df.drop('label', axis=1).corr(numeric_only=True).fillna(0))
        fig.update_layout(autosize=False, width=500, height=350, margin=self.margins)
        self.corr_plot = fig.to_html(include_plotlyjs='require', full_html=False)

    def similarity_graph(self):
        fig = px.imshow(self.gow)
        fig.update_layout(autosize=False, width=400, height=350, margin=self.margins)
        return fig.to_html(include_plotlyjs='require', full_html=False)

    md_selection = '''
    The system advises the client to take a look into the following records. These records where eighter flagged as dangerous by a flag indicator or they were recognized as anormal by classification engine.
    '''

    md_suspects = '''
    ## Suspicious Dataset

    These subsets containe the suspects records according to the selected features and flags.
    '''

    md_main = '''
    ## Data Clustering

    These graphs show the clustering of the data.

    Use the slider to increase the cluster segmentation. The dendrogram can help to visualize the branching of the clustering.
    '''

    md_main_2 = '''
    ## Similarity Heatmap

    This heatmap shows the Similarity Matrix using the Gower metric.

    This can be helpful to visualize entries that are similar to each other.
    '''

    md_features_2 = '''
    ## Anomal Group Characteristics

    The anomal group has the following characteristics:
    '''

    md_features_3 = '''
    ## Feature weights

    In these plots you can see the user assigned weights to each feature vs the final contribution of each feature to the labeling.
    '''

    md_features_4 = '''
    ## Feature Density

    Click on each feature to see how was the data distributed according to that feature. This could be helpful to understand the usefulness of a feature when separating the data.
    '''

    md_features_5 = '''
    ## Correlation Matrix

    This is the correlation matrix between the features.

    The heatmap can be helpful to detect features that are similar to each other.
    '''

    def show_popup(self, flag_result_df):
        popup('Flag Details', [
            put_scrollable(put_html(tabulate(flag_result_df, headers='keys', tablefmt='html'))),
            put_button('Close', onclick=close_popup, outline=True)
        ], size=PopupSize.LARGE)

    def get_scrollables(self):
        return [put_collapse(f['name'], [
                    put_markdown(f['description']).style('color:white'),
                    put_row([
                        put_markdown(f['message']).style('color:white'),
                        put_button('Details', onclick=partial(self.show_popup, f['result'])),
                        put_button('Download', onclick=partial(download, f['name'] + '_result.csv', bytes(f['result'].to_csv(lineterminator='\n', index=False), encoding='utf-8')))
                    ], size='70% 13% 17%')
        ]).style(self.styles[f['severity']]) for f in self.flags]

    def get_tabs(self):
        flag_tab = {'title' : 'Flags Detector',
             'content':
             put_column(self.get_scrollables(), size='repeat(auto-fill, auto)')
        }
        if not self.only_flags:
            main_tab = {'title': 'Main',
                 'content': [
                    put_column([
                        put_markdown(self.md_main),
                        put_row([
                            put_column([put_html(self.dend_plot)]),
                            put_scope('bokeh')
                        ]).style('margin-top:-90px')
                    ]),
                    put_column([
                        put_markdown(self.md_main_2),
                        put_html(self.similarity_graph()).style('margin-top:-100px;margin-left:200px')
                    ])
                ]}
            behavior_tab = {'title': 'Behavior Analysis',
                 'content':[
                        put_markdown(self.md_features),
                        put_markdown(self.md_features_2),
                        put_html(self.suspects_features_group()).style('max-width:800px;max-height:180px'),
                     put_markdown(self.md_features_3).style('margin-top:20px'),
                        put_row([
                            put_table([[x,y] for x,y in zip(self.df.columns, self.weights)]
                                      , header=['Feature', 'Weight']).style('margin-top:20px'),
                            put_html(self.feature_relevance_graph()).style('margin-bottom:10px')
                        ]),
                        put_row([
                            put_column([
                                put_markdown(self.md_features_4),
                                put_select(name='distplot', options=self.df.columns.drop('label'), value=self.df.columns[0])]),
                            put_scope('distplot').style('margin-top:70px;margin-left:10px'),
                        ]).style('margin-top:-20px'),
                        put_column([
                            put_markdown(self.md_features_5),
                            put_html(self.corr_plot).style('margin-top:-90px;margin-left:150px;')
                        ])
                ]}
            return [ main_tab , behavior_tab, flag_tab ]
        return [flag_tab]

    @property
    @config(css_style="* {color:red}")
    def server_main(self):
        output_notebook(verbose=True, notebook_type='pywebio')
        put_image('https://pbs.twimg.com/media/FdIbN_nWYAgaGkl?format=png&name=small').style('height:300px;width:300px;margin-left:250px')
        put_markdown(self.md_selection).style('margin-bottom:30px')
        if not self.only_flags:
            put_markdown(self.md_intro)
            original_malicious_df, feature_malicious_df = self.get_suspects()
            put_markdown(self.md_suspects),
            put_tabs([
                {'title': 'Original Dataset',
                 'content': [
                    put_button('Download', onclick=partial(download, 'suspects_original_df.csv', bytes(original_malicious_df.to_csv(lineterminator='\n', index=False), encoding='utf-8'))),
                    put_scrollable(put_html(original_malicious_df.to_html(border=0)))
                 ]
                },
                {'title': 'Feature Dataset',
                 'content': [
                    put_button('Download', onclick=partial(download, 'suspects_feature_df.csv', bytes(feature_malicious_df.to_csv(lineterminator='\n', index=False), encoding='utf-8'))),
                     put_scrollable(put_html(feature_malicious_df.to_html(border=0)))
                 ]
                }
            ])
        put_tabs(self.get_tabs())
        if not self.only_flags:
            with use_scope('bokeh'):
                show(self.bkapp)

            with use_scope('distplot'):
                put_html(self.build_distplot_by_feature(self.df.columns[0]))

            while True:
                changed = pin_wait_change('distplot')
                with use_scope('distplot', clear=True):
                    put_html(self.build_distplot_by_feature(changed['value']))

    def bake_data(self):
        self.build_dendogram()
        self.build_corr_plot()

    def start(self):
        config(theme='dark')
        start_server(self.server_main, port=8080, debug=False, auto_open_webbrowser=False)
