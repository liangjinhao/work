import itertools
import numpy as np
import matplotlib.pyplot as plt
from sklearn.metrics import confusion_matrix


"""
================
Confusion matrix
================
the input data are all np.array,the result path is the predict file,colums are:word,real tag,predict tag
by the way,the tag must count from 0,not 1 or any other numbers.
"""

print(__doc__)


def get_label(predict_path):
    f = open(predict_path, 'r')
    real = []
    predict = []
    for line in f.readlines():
        if line == '\n':
            continue
        print(line)
        real.append(line.split('\t')[-2])
        predict.append(line.split('\t')[-1])
    f.close()
    real = [int(i)-1 for i in real]
    predict = [int(i)-1 for i in predict]
    return np.array(real), np.array(predict)


def plot_confusion_matrix(cm, classes,
                          normalize=False,
                          title='Confusion matrix',
                          cmap=plt.cm.Blues):
    """
    This function prints and plots the confusion matrix.
    Normalization can be applied by setting `normalize=True`.
    """
    if normalize:
        cm = cm.astype('float') / cm.sum(axis=1)[:, np.newaxis]
        print("Normalized confusion matrix")
    else:
        print('Confusion matrix, without normalization')

    print(cm)

    plt.imshow(cm, interpolation='nearest', cmap=cmap)
    plt.title(title)
    plt.colorbar()
    tick_marks = np.arange(len(classes))
    plt.xticks(tick_marks, classes, rotation=45)
    plt.yticks(tick_marks, classes)

    fmt = '.2f' if normalize else 'd'
    thresh = cm.max() / 2.
    for i, j in itertools.product(range(cm.shape[0]), range(cm.shape[1])):
        plt.text(j, i, format(cm[i, j], fmt),
                 horizontalalignment="center",
                 color="white" if cm[i, j] > thresh else "black")

    plt.tight_layout()
    plt.ylabel('True label')
    plt.xlabel('Predicted label')


def plot_result(resultPath, className):
    class_names = np.array(className)
    y, y_pred = get_label(resultPath)
    cnf_matrix = confusion_matrix(y, y_pred)
    np.set_printoptions(precision=2)

    # Plot non-normalized confusion matrix
    plt.figure()
    plot_confusion_matrix(cnf_matrix, classes=class_names, title='Confusion matrix, without normalization')
    # Plot normalized confusion matrix
    plt.figure()
    plot_confusion_matrix(cnf_matrix, classes=class_names, normalize=True, title='Normalized confusion matrix')
    plt.show()


if __name__ == '__main__':
    className = ['subject1', 'subject2', 'subject3', 'indicator', 'product', 'area', 'formula', 'useless', 'time']
    resultPath = 'crf/test_data_predict'
    plot_result(resultPath, className)
