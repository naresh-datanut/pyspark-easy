from pyspark_easy.utils import *

from sklearn.metrics import confusion_matrix

from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score, roc_auc_score

import sklearn.metrics as metrics

from texttable import Texttable


class pyspark_model_eval():

    def __init__(self, model, predicted_df):

        if 'pyspark.ml.classification' in str(type(model)):

            self.type_of_model = model.numClasses

            self.label_column = model.summary.labelCol

            self.y_true = predicted_df.select([self.label_column]).rdd.flatMap(lambda x: x).collect()

            self.y_pred = predicted_df.select(['prediction']).rdd.flatMap(lambda x: x).collect()

            self.yp_pred = predicted_df.select(['probability']).rdd.flatMap(lambda x: x).collect()

            self.cm = confusion_matrix(self.y_true, self.y_pred)

            self.model = model

        else:

            print("This is not a Classification Model")

    def results(self):

        t = Texttable()

        train_test = []

        train_test.append(['Metrics', 'Train', 'Test'])

        if hasattr(self.model.summary, 'areaUnderROC'):
            train_roc = self.model.summary.areaUnderROC

            test_roc = roc_auc_score(self.y_true, self.y_pred)

            train_test.append(['AreaUnderROC', round(train_roc * 100, 2), round(test_roc * 100, 2)])

        train_accuracy = self.model.summary.accuracy

        test_accuracy = accuracy_score(self.y_true, self.y_pred)

        train_test.append(['Accuracy', round(train_accuracy * 100, 2), round(test_accuracy * 100, 2)])

        train_precision = self.model.summary.weightedPrecision

        test_precision = precision_score(self.y_true, self.y_pred, average='weighted')

        train_test.append(['Weighted Precision', round(train_precision * 100, 2), round(test_precision * 100, 2)])

        train_recall = self.model.summary.weightedRecall

        test_recall = recall_score(self.y_true, self.y_pred, average='weighted')

        train_test.append(['Weighted Recall', round(train_recall * 100, 2), round(test_recall * 100, 2)])

        train_f1score = self.model.summary.weightedFMeasure()

        test_f1score = f1_score(self.y_true, self.y_pred, average='weighted')

        train_test.append(['Weighted F1 Score', round(train_f1score * 100, 2), round(test_f1score * 100, 2)])

        t.add_rows(train_test)

        print("The Train vs Test evaluation Metrics are :")

        print(t.draw())

        print("Note: The Weighted metrics - calculated by label then it is averaged")

        plot_cm(self.cm)

        if hasattr(self.model.summary, 'roc'):
            roc = self.model.summary.roc

            training_roc(roc)

        if hasattr(self.model.summary, 'pr'):
            pr = self.model.summary.pr

            training_pr(pr)

        if self.type_of_model == 2:
            fpr, tpr, threshold = metrics.roc_curve(self.y_true, self.y_pred)

            roc_auc = metrics.auc(fpr, tpr)

            ROC(fpr, tpr, roc_auc)

            precision, recall, thresholds = metrics.precision_recall_curve(self.y_true, self.y_pred)

            auc = metrics.auc(recall, precision)

            PR(recall, precision, auc)

        if self.type_of_model > 2:

            classes = np.unique(self.y_true)

            i = 0

            j = 5

            while True:

                plt_roc(self.y_true, self.yp_pred, classes=classes[i:j])

                plt_pr(self.y_true, self.yp_pred, classes=classes[i:j])

                if j > len(classes):

                    break

                else:

                    i = i + 5

                    j = j + 5

        else:

            plt_roc(self.y_true, self.yp_pred)

            plt_pr(self.y_true, self.yp_pred)

        if self.type_of_model == 2:
            plt_lift(self.y_true, self.yp_pred)

            plt_cumulative(self.y_true, self.yp_pred)

            t = Texttable()

            train_test = []

            train_test.append(['Metrics', 'Test Data'])

            precision = precision_score(self.y_true, self.y_pred)

            train_test.append(['Precision', round(precision * 100, 2)])

            recall = recall_score(self.y_true, self.y_pred)

            train_test.append(['Recall', round(recall * 100, 2)])

            f_score = f1_score(self.y_true, self.y_pred)

            train_test.append(['F1 Score', round(f_score * 100, 2)])

            t.add_rows(train_test)

            print("The binary classification metrics for test data are:")

            print(t.draw())

            print("\n")

        print("The classification report follows :")

        return classif_report(self.y_true, self.y_pred)