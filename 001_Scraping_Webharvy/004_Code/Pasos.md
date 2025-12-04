PASOS PARA EJECUTAR EL ESCRAPEO DE FACEBOOK EN OTRO COMPUTADOR

1. PERMISOS DE POWERSHELL
    - Se abre powershell con permisos de administrador y se ejecuta el siguiente comando: Set-ExecutionPolicy RemoteSigned
    - Después se escribe "Y" y se le da enter.

2. CREACION DEL ENTORNO VIRTUAL DE PYTHON 
    - Se ubica en la carpeta 004_Code y se ejecuta el siguiente comando para crear el entorno virtual: python -m venv .venv
    - Después se activa el entorno usando el siguiente comando:  ".venv\Scripts\Activate.ps1"
    - Se instalan las librerias necesarias usando el comando: pip install -r requirements.txt
    - IMPORTANTE: Asegúrate de que el entorno virtual esté activado (debe aparecer (.venv) en el prompt) antes de ejecutar main.py