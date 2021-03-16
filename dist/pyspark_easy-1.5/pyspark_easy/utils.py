###########################################################
import numpy as np

import seaborn as sns

import matplotlib.pyplot as plt

import scikitplot as skplt

import pandas as pd

from sklearn.metrics import classification_report

#Util functions 

def Extract(lst): 
    return [item[0] for item in lst]


fontdict = {'fontsize': 9, 'fontweight': 'medium'}


def training_roc(roc):
    roc = roc.toPandas()

    plt.plot(roc['FPR'], roc['TPR'])

    plt.ylabel('False Positive Rate', fontdict=fontdict, color='black')

    plt.xlabel('True Positive Rate', fontdict=fontdict, color='black')

    plt.title('Training - ROC Curve', fontsize=10, weight='bold', color='steelblue')

    plt.plot([0, 1], [0, 1], 'k--')

    plt.xticks([0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0], fontsize=8, weight='bold', color='darkslategrey')

    plt.yticks([0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0], fontsize=8, weight='bold', color='darkslategrey')

    plt.show()


def training_pr(pr):
    pr = pr.toPandas()

    plt.plot(pr['recall'], pr['precision'])

    plt.ylabel('Precision', fontdict=fontdict, color='black')

    plt.xlabel('Recall', fontdict=fontdict, color='black')

    plt.title('Training - PR Curve', fontsize=10, weight='bold', color='steelblue')

    plt.plot([0, 1], [0, 1], 'r--')

    plt.xticks([0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0], fontsize=8, weight='bold', color='darkslategrey')

    plt.yticks([0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0], fontsize=8, weight='bold', color='darkslategrey')

    plt.show()


def plot_cm(cm):

 fig_len = 1 + cm.shape[0]

 fig_wid = round(fig_len * 0.75)

 fig, ax = plt.subplots(figsize=(fig_len, fig_wid))

 sns.set(font_scale=0.75)

 cm_sum = np.sum(cm, axis=1, keepdims=True)

 cm_perc = cm / cm_sum.astype(float) * 100

 annot = np.empty_like(cm).astype(str)

 nrows, ncols = cm.shape

 for i in range(nrows):

    for j in range(ncols):

        c = cm[i, j]

        p = cm_perc[i, j]

        if i == j:

            s = cm_sum[i]

            annot[i, j] = '%d\n\n(%.1f%%)' % (c, p)

        elif c == 0:

            annot[i, j] = '%d' % c

        else:

            annot[i, j] = '%d\n\n(%.1f%%)' % (c, p)

 sns.heatmap(cm, annot=annot, cmap='Blues', xticklabels='auto', cbar=True,
            annot_kws={"fontsize": 8, 'fontweight': 'medium'}, fmt='', ax=ax);  # annot=True to annotate cells

# labels, title and ticks

 ax.set_xlabel('Predicted labels', fontdict=fontdict, color='slategrey')

 ax.set_ylabel('True labels', fontdict=fontdict, color='slategrey')

 ax.set_title('Confusion Matrix', fontsize=10, weight='bold', color='steelblue')

 plt.xticks(fontsize=8, weight='bold', color='darkslategrey')

 plt.yticks(fontsize=8, weight='bold', color='darkslategrey')

 plt.show()


def classif_report(y_true, y_pred):
    df = pd.DataFrame(classification_report(y_true, y_pred,

                                            output_dict=True)).T

    for i in df.columns:
        df[i] = df[i].round(decimals=2)

    df['support'] = df.support.apply(int)

    return df.style.background_gradient(cmap='YlGn',

                                        subset=pd.IndexSlice[df.index[:-3], :'f1-score'])


def plt_lift(y_true, yp_pred):

 skplt.metrics.plot_lift_curve(y_true, yp_pred)

 plt.title('Lift Curve', fontsize=10, weight='bold', color='steelblue')

 plt.ylabel('Lift', fontdict=fontdict, color='black')

 plt.xlabel('Percentage of Sample', fontdict=fontdict, color='black')

 plt.show()


def plt_cumulative(y_true, yp_pred):

 skplt.metrics.plot_cumulative_gain(y_true, yp_pred)

 plt.title('Cumulative Gains Curve', fontsize=10, weight='bold', color='steelblue')

 plt.ylabel('Gain', fontdict=fontdict, color='black')

 plt.xlabel('Percentage of Sample', fontdict=fontdict, color='black')

 plt.show()


def plt_roc(y_true, yp_pred, classes=None):

 skplt.metrics.plot_roc(y_true, yp_pred, classes_to_plot=classes, plot_micro=False, plot_macro=False)

 plt.title('Test Data - Receiver Operating Characteristic by Class', fontsize=10, weight='bold', color='steelblue')

 plt.xticks([0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0], fontsize=8, weight='bold', color='darkslategrey')

 plt.yticks([0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0], fontsize=8, weight='bold', color='darkslategrey')

 plt.show()


def plt_pr(y_true, yp_pred, classes=None):

 skplt.metrics.plot_precision_recall(y_true, yp_pred, classes_to_plot=classes, plot_micro=False)

 plt.plot([0, 1], [0, 1], 'k--')

 plt.title('Test Data - Precision-Recall Curve by Class', fontsize=10, weight='bold', color='steelblue')

 plt.xticks([0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0], fontsize=8, weight='bold', color='darkslategrey')

 plt.yticks([0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0], fontsize=8, weight='bold', color='darkslategrey')

 plt.show()


def ROC(fpr, tpr, roc_auc):

 legend_properties = {'weight': 'bold', 'size': 8}

 plt.title('Test Data - Receiver Operating Characteristic', fontsize=10, weight='bold', color='steelblue')

 plt.plot(fpr, tpr, 'b', label='AUC = %0.2f' % roc_auc)

 plt.legend(loc='lower right', prop=legend_properties)

 plt.plot([0, 1], [0, 1], 'k--')

 plt.xticks([0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0], fontsize=8, weight='bold', color='darkslategrey')

 plt.yticks([0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0], fontsize=8, weight='bold', color='darkslategrey')

 plt.ylabel('True Positive Rate', fontdict=fontdict, color='black')

 plt.xlabel('False Positive Rate', fontdict=fontdict, color='black')

 plt.show()


def PR(recall, precision, auc):

 legend_properties = {'weight': 'bold', 'size': 8}

 plt.title('Test Data - Precision-Recall Curve', fontsize=10, weight='bold', color='steelblue')

 plt.plot(recall, precision, 'b', label='AUC = %0.2f' % auc)

 plt.legend(loc='lower right', prop=legend_properties)

 plt.plot([0, 1], [0, 1], 'r--')

 plt.xticks([0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0], fontsize=8, weight='bold', color='darkslategrey')

 plt.yticks([0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0], fontsize=8, weight='bold', color='darkslategrey')

 plt.ylabel('Precision', fontdict=fontdict, color='black')

 plt.xlabel('Recall', fontdict=fontdict, color='black')

 plt.show()