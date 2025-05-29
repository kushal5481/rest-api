@echo off

:: Specify the source and destination details of the file
set "sourceFolder=C:\Users\C19269E\Downloads\Softwares_Zip\"
set "zipFile=log4j-core-2.17.1.zip"
set "unzippedFile=log4j-core-2.17.1.jar"
set "destinationFolders=C:\NG_Git\lib;C:\Users\C19269E\AppData\Local\Programs\jboss-eap-8.0\modules\com\experian\bos\main;C:\Users\C19269E\AppData\Local\Programs\jboss-eap-8.0\modules\com\experian\client\main;C:\Users\C19269E\AppData\Local\Programs\jboss-eap-8.0\modules\com\experian\pds\main;C:\Users\C19269E\AppData\Local\Programs\jboss-eap-8.0\modules\com\experian\pds-service\main"

echo Source Folder is %sourceFolder%

echo Zip file is %zipFile%

:: Change directory to the source folder
cd /d "%sourceFolder%"

:: Unzip the file in the same location
powershell -command "Expand-Archive -Path '%zipFile%' -DestinationPath '.'"

:: Initialize a flag to check if the file was moved
set "fileMoved=0"

:: Move the unzipped file to each destination folder
for %%D in (%destinationFolders%) do (
    if exist "%%D\%unzippedFile%" (
        echo %unzippedFile% already exists in %%D, skipping...
    ) else (
        move "%unzippedFile%" "%%D"
        echo %unzippedFile% has been moved to %%D
		set "fileMoved=1"
    )
)

:: Check if the file was moved; if not, delete the unzipped file
if "%fileMoved%"=="0" (
    del "%unzippedFile%"
    echo %unzippedFile% was not moved and has been deleted
)

pause

