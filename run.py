import gui
import remi

if __name__ == "__main__":
    remi.start(gui.MultiDbQueryFrontEnd, address='127.0.0.1', port=8081, multiple_instance=False, enable_file_cache=True,
               update_interval=0.1, start_browser=True)
