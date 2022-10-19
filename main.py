import ui
import sys
import os

if __name__=="__main__":
    print('start')

    app = ui.QtWidgets.QApplication(sys.argv)
    window = ui.Ui()
    window.show()
    app.exec_()