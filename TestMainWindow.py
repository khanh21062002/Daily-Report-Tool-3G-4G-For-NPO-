from PyQt6.QtWidgets import QApplication, QMainWindow
# import sys for Accessing to command line arguments
import sys

from main_window import Ui_MainWindow

# You need one (and only one) QApplication instance per application.
# Pass in sys.argv to allow command line arguments for your app.

app=QApplication(sys.argv)
# If you know you won't use command line arguments QApplication([]) works too
#app=QApplication([])

#Create QMainWindow
qMainWindow=QMainWindow()
#Call Ui_MainWindow in the MyFirstApplication.py, of course we can change the name
myWindow=Ui_MainWindow()
#Call setupUi method
myWindow.setupUi(qMainWindow)
#call show method
qMainWindow.show()
#Start the Event loop
app.exec()