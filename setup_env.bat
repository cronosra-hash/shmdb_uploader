@echo off
REM Setup Python virtual environment for shmdb_uploader

REM Create virtual environment in 'venv' folder
python -m venv venv

REM Activate the virtual environment
call venv\Scripts\activate.bat

REM Upgrade pip
python -m pip install --upgrade pip

REM Install dependencies from requirements.txt
pip install -r requirements.txt

echo.
echo ✅ Virtual environment setup complete!
echo To activate it later, run:
echo     venv\Scripts\activate.bat
