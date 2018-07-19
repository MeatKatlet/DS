import numpy as np
import matplotlib
import matplotlib.pyplot as plt
import warnings
import pandas as pd
import elastic.elastic_queries
warnings.filterwarnings('ignore')
#from searches.elastic_queries import Base_Elastic
#from searches.elastic_queries import Searches_in_input_field_event
#from searches.elastic_queries import Search_results_events

#from searches.elastic_queries import run_logic
#from searches.elastic_queries import region_brand


def heatmap(data, row_labels, col_labels, ax=None,
            cbar_kw={}, cbarlabel="", **kwargs):
    """
    Create a heatmap from a numpy array and two lists of labels.

    Arguments:
        data       : A 2D numpy array of shape (N,M)
        row_labels : A list or array of length N with the labels
                     for the rows
        col_labels : A list or array of length M with the labels
                     for the columns
    Optional arguments:
        ax         : A matplotlib.axes.Axes instance to which the heatmap
                     is plotted. If not provided, use current axes or
                     create a new one.
        cbar_kw    : A dictionary with arguments to
                     :meth:`matplotlib.Figure.colorbar`.
        cbarlabel  : The label for the colorbar
    All other arguments are directly passed on to the imshow call.
    """

    if not ax:
        ax = plt.gca()

    # Plot the heatmap
    im = ax.imshow(data, **kwargs)

    # Create colorbar
    cbar = ax.figure.colorbar(im, ax=ax, **cbar_kw)
    cbar.ax.set_ylabel(cbarlabel, rotation=-90, va="bottom")

    # We want to show all ticks...
    ax.set_xticks(np.arange(data.shape[1]))
    ax.set_yticks(np.arange(data.shape[0]))
    # ... and label them with the respective list entries.
    ax.set_xticklabels(col_labels)
    ax.set_yticklabels(row_labels)

    # Let the horizontal axes labeling appear on top.
    ax.tick_params(top=True, bottom=False,labeltop=True, labelbottom=False)

    # Rotate the tick labels and set their alignment.
    plt.setp(ax.get_xticklabels(), rotation=-30, ha="right",rotation_mode="anchor")

    # Turn spines off and create white grid.
    for edge, spine in ax.spines.items():
       spine.set_visible(False)

    ax.set_xticks(np.arange(data.shape[1]+1)-.5, minor=True)
    ax.set_yticks(np.arange(data.shape[0]+1)-.5, minor=True)
    ax.grid(which="minor", color="w", linestyle='-', linewidth=3)
    ax.tick_params(which="minor", bottom=False, left=False)

    return im#, cbar

def annotate_heatmap(im, data=None, valfmt="{x:.2f}",
                     textcolors=["black", "white"],
                     threshold=None, **textkw):
    """
    A function to annotate a heatmap.

    Arguments:
        im         : The AxesImage to be labeled.
    Optional arguments:
        data       : Data used to annotate. If None, the image's data is used.
        valfmt     : The format of the annotations inside the heatmap.
                     This should either use the string format method, e.g.
                     "$ {x:.2f}", or be a :class:`matplotlib.ticker.Formatter`.
        textcolors : A list or array of two color specifications. The first is
                     used for values below a threshold, the second for those
                     above.
        threshold  : Value in data units according to which the colors from
                     textcolors are applied. If None (the default) uses the
                     middle of the colormap as separation.

    Further arguments are passed on to the created text labels.
    """

    if not isinstance(data, (list, np.ndarray)):
        data = im.get_array()

    # Normalize the threshold to the images color range.
    if threshold is not None:
        threshold = im.norm(threshold)
    else:
        threshold = im.norm(data.max())/2.

    # Set default alignment to center, but allow it to be
    # overwritten by textkw.
    kw = dict(horizontalalignment="center",
              verticalalignment="center")
    kw.update(textkw)

    # Get the formatter in case a string is supplied
    if isinstance(valfmt, str):
        valfmt = matplotlib.ticker.StrMethodFormatter(valfmt)

    # Loop over the data and create a `Text` for each "pixel".
    # Change the text's color depending on the data.
    texts = []
    for i in range(data.shape[0]):
        for j in range(data.shape[1]):
            kw.update(color=textcolors[im.norm(data[i, j]) > threshold])
            text = im.axes.text(j, i, valfmt(data[i, j], None), **kw)
            texts.append(text)


def plot_heatmap(percentage_of_sucsess,title):

    fig, ax = plt.subplots(figsize=(26, 26))
    # x = percentage_of_sucsess.values
    # x.astype(float)
    df = percentage_of_sucsess
    percentage_of_sucsess = percentage_of_sucsess[percentage_of_sucsess.columns].astype(float)

    percentage_of_sucsess = percentage_of_sucsess.loc[(percentage_of_sucsess != 0).any(axis=1)]
    percentage_of_sucsess = percentage_of_sucsess.loc[:, (percentage_of_sucsess != 0).any(axis=0)]

    row_labels = list(percentage_of_sucsess.index)
    col_labels = list(percentage_of_sucsess.columns)

    im = heatmap(percentage_of_sucsess.values, row_labels, col_labels, ax=ax, cmap="YlGn",cbarlabel="%, удачных поисков")
    texts = annotate_heatmap(im, valfmt="{x:.1f}")
    plt.title(title, y=1.08)
    fig.tight_layout()
    plt.show()

    df = df.loc[(df != 0).any(axis=1)]
    df = percentage_of_sucsess.loc[:, (df != 0).any(axis=0)]
    print(df.columns)
    print("\n--------------------\n")
    print(df.index)



all_searches,n = elastic.elastic_queries.run_logic(True)#надо сделать чтобы лидо запрос из бд, и результатом будет фрейм, который мы сохраняем в файл, либо считываем из файла фрейм
#по каждому сочетанию сделать heatmap
"""
география - бренды
география - товарные группы
бренды - товарные группы
figsize=(160, 17)
harvest = np.array([[0.8, 0.0, 2.5, 3.9, 0.0, 4.0, 0.0],
                    [2.4, 0.0, 4.0, 1.0, 2.7, 0.0, 0.0],
                    [1.1, 0.0, 0.8, 4.3, 1.9, 4.4, 0.0],
                    [0.6, 0.0, 0.3, 0.0, 3.1, 0.0, 0.0],
                    [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
                    [1.3, 0.0, 0.0, 0.0, 0.0, 3.2, 5.1],
                    [0.1, 0.0, 0.0, 1.4, 0.0, 1.9, 6.3]])


#
df = pd.DataFrame(harvest)
df = df.loc[(df!=0).any(axis=1)]
df = df.loc[:, (df != 0).any(axis=0)]
a= 14

"""


"""
harvest = np.random.rand(45,737)


fig, ax = plt.subplots(figsize=(160, 17))#figsize=(15, 15)
    #x = percentage_of_sucsess.values
    #x.astype(float)

im = heatmap(harvest, vegetables, farmers, ax=ax, cmap="YlGn",cbarlabel="%, удачных поисков")
texts = annotate_heatmap(im, valfmt="{x:.1f}")

fig.tight_layout()
plt.show()
"""


#+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
f = elastic.elastic_queries.region_brand
percentage_of_sucsess = elastic.elastic_queries.calc_percentage(all_searches,f, n)#регион-бренд


plot_heatmap(percentage_of_sucsess, '% удачных поисков. Регион vs Бренд')


#++++++++++++++++++++++
f = elastic.elastic_queries.region_group
percentage_of_sucsess = elastic.elastic_queries.calc_percentage(all_searches, f, n)#регион-группа

plot_heatmap(percentage_of_sucsess, '% удачных поисков. Регион vs Товарная группа')


#+++++++++++++++++++++++++++++++++
f = elastic.elastic_queries.brand_group
percentage_of_sucsess = elastic.elastic_queries.calc_percentage(all_searches, f, n)#бренд-группа

plot_heatmap(percentage_of_sucsess, '% удачных поисков, Бренд vs Товарная группа')






