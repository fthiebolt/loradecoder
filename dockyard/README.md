# dockyard dir | container customisation #
__________________________________________

This directory hosts severall files / dirs that will be copied to the container @ creation time.

## Adding SSH keys ##
You can add your public key the `authorized_keys` file that will get copied to the `/root/.ssh/authorized_keys`.  
Hence, you 'll be able to ssh as root within the container:
```
ssh -p xxxx root@localhost
```  
## Python dependencies ##
Put additional requiered python modules within the `requirements.txt` file:
```
django
supervisord
```
You can also ask for a specific version of a module, the same way `pip freeze` tells the modules you are currently using.

## root dir ##
Just copying files to the `/root` directory within the container  

## supervisord.d ##
Since container can only start **one application**, `supervisord` implement this one as a daemon management system.  
The various files `sshd.ini` and `*.ini` files are applications (**NOT DAEMONS**) launched by supervisord.

*Note: it is important not to launch applications within supervsisord as daemons (thus why sshd -D), because supervisord will try to make them to behave like daemons, relaunched them as required.*

