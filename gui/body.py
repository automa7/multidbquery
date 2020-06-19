import remi.gui as gui
import gui.styler as wid
from remi import App


class MultiDbQueryFrontEnd(App):
    def main(self):
        main_container = gui.Container(width=800, height=600, margin='0px auto', style="position: relative")
        vbox1 = gui.VBox()
        vbox2 = gui.VBox()
        vbox3 = gui.VBox()

        vbox1.append(wid.DatabaseList)
        vbox2.append(wid.QueryBox)
        vbox3.append(wid.ServerBtn)
        vbox3.append(wid.ExecuteBtn)

        main_container.append(vbox1)
        main_container.append(vbox2)
        main_container.append(vbox3)

        return main_container


