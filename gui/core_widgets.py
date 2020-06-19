from remi.gui import *
from remi import *
# Declare
ServerBtn = Button('Server')
ExecuteBtn = Button('Execute')
QueryBox = TextInput(width='40%', height='300px', margin='10px', single_line=False)
DatabaseList = ListView()

server_url = TextInput()
server_user = TextInput()
server_password = TextInput()
server_driver = TextInput()

