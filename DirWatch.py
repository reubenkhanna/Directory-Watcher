"""
Utkarsh Reuben Khanna
C0750077
"""
#--------------Imports --------------------------------------------------------------#
import sys
import os
import time 
import logging
import argparse
import sqlite3
import datetime 
import shutil
#--------------------------------------------------------------------------------------#

sqlite_file = 'DirWatchEvents.sqllite'    # name of the sqlite database file
conn = sqlite3.connect(sqlite_file)       # Connecting to the database file
c = conn.cursor()                         # Pointer to database 
# Creating a new SQLite table if not already exist
c.execute('CREATE TABLE IF NOT EXISTS events (date date ,time time ,incoming_file_path  varchar, outgoing_file_path varchar,action varchar)')
# A placeholder query for insert statement
statement = "INSERT INTO events (date, time, incoming_file_path,outgoing_file_path,action) values (?,?,?,?,?)"

#------------Logger Info----------------------------------------------------------------#
logging.basicConfig(format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',          #Format of logging 
                    datefmt='%H:%M:%S',                                                         #Date display format 
                    level=logging.DEBUG,                                                        #Logg level to debug
                    handlers=[                                                                  #Handlers
                        logging.FileHandler("DirWatchLog","a"),                                 #Create a log file DirWatchLog in append mode
                        logging.StreamHandler()                                                 #Create a Stream Handler for logging on console 
                    ])
#----------------------------------------------------------------------------------------#

#------------Argument Parser--------------------------------------------------------------#
parser=argparse.ArgumentParser()                                                                            #Create a arugment parser 
group = parser.add_mutually_exclusive_group()                                                               #Create mututally exclusive group 
group.add_argument('-M', action='store_true',help="Moves file from input directory to output directory")    #Add Move command using -M 
group.add_argument('-C', action='store_true',help="Copies file from input directory to output directory")   #Add Copy Command using -C
parser.add_argument('-i', help="Directory to watch", required=True)                                         #Add -i for input which is required 
parser.add_argument('-o', help="Directory to output to.", required= True)                                   #Add -i for output which is required 
args = parser.parse_args();                                                                                 #Parse the arguments 
if(args.C == False and args.M == False):                                                                    #If Copy and Move are not provided as flag 
    args.C = True                                                                                           #Set Copy to true

if(args.i == args.o):
    raise  ValueError("Output and Input Can't be same")                                                     #Raising error if output and input are same

#-------------------------------------------------------------------------------------------#
path_to_watch = args.i                              #Stored Path to be watched
path_to_output_to = args.o                          #Sotred path to be copied or move to 

#Following statement logs the event of operation started by user 
c.execute(statement,(time.strftime('%Y-%m-%d'),time.strftime('%H:%M:%S') ,path_to_watch,path_to_output_to,("C" if args.C else "M")))
conn.commit()                                       #Commit the query

#----------Initial Logggers---------------------------------------------------------------#
logging.info("Started Directory Watcher....")
logging.info("Directory Watcher Mode Set To " +("Copy" if args.C else "Move")  )
logging.info("Watcher set on directory "+ args.i)
logging.info("Output Directory set to "+ args.o)
#----------------------------------------------------------------------------------------#

#------- Get File Path Method-----------------------------------------------------------#
def getFilePaths(directory):
    paths =[]                                                          #Used to append paths to it
    for root, dirs, files in os.walk(directory):                       #Walk through the directory and get root, directories and files
        for file in files:                                             #For each file
            paths.append("{}\{}".format(root,file))                    #Append file with its root path  to paths list
    return paths                                                       #Return paths 
#-----------------------------------------------------------------------------------------#

#--------------Get Direcotry Path methdo --------------------------------------------------#
def getDirPaths(directory):
    directories =[]                                                    #Used to append directory 
    for root, dirs, files in os.walk(directory):                       #Walk through the directory and get root, directories and files
        for dir in dirs:                                               #For each directory 
            directories.append("{}\{}".format(root,dir))               #Append directory with its rooth path to directories list 
    return directories                                                 #Return directories 
#------------------------------------------------------------------------------------------#

#Pre run to create a list of path which exists in output folder 

# Here we use dictionary and save each file as a key value pair 
# where key is the file path itself and value is set to none
# The reason to use dictionary is faster searching which we will see in the while loop
# Here we first get what contents are available in output folder and store it 


file_before_check =dict([(f.replace(args.o,""),None) for f in getFilePaths(path_to_output_to)])          #Calls getFilePath method add absolute file path of output folder to list 
directory_before_check =dict([(d.replace(args.o,""),None) for d in getDirPaths(path_to_output_to)])      #Calls getDirPath method add absolute directory path of output folder to list 

#----Commit to DB -----------------------------------------------------------------------#
def commitToDB(input, output, action):
    c.execute(statement,(time.strftime('%Y-%m-%d'),                                     #Add Current Date
                time.strftime('%H:%M:%S') ,                                             #Add Current time
                input,                                                                  #Input file we get from 
                output,                                                                 #File we are sending to 
                action))                                                                #Action being performed 
    conn.commit()
#----------------------------------------------------------------------------------------#

#--------Perorm Action Method -------------------------------------------------------------#
def moveFile(files):
    if args.C:                                                                  #Check if the option is to copy
        for file in files:                                                      #For each file in files
            output = r"{}\{}".format(args.o,file)                               #append output path to file        
            input = r"{}\{}".format(args.i,file)                                #append input path to file        
            
            if(os.path.exists(os.path.split(output)[0])):                       #Check If the directory exist  in output    
                logging.info("Copy file from " + input+ " to " + output)        #Log the copy action                                               
                shutil.copy2(input,output)                                      #Copy input to output folder
                commitToDB(input,output,"C")                                    #Commiting to DB
            else:                                                               #If Direcotry doesnt exists in output
                os.makedirs(os.path.split(output)[0])                           #Make the directory if it doesn't exists 
                logging.info("Copy file from " + input+ " to " + output)        #Log the copy action                          
                shutil.copy2(input,output)                                      #Copy input to output folder 
                commitToDB(input,output,"C")                                    #Commiting to DB
    else:                                                                       #If Argument is move 
        for file in files:                                                      #For each file in file 
            output = r"{}\{}".format(args.o,file)                               #append output path to file        
            input = r"{}\{}".format(args.i,file)                                #append input path to file         
            if(os.path.exists(os.path.split(output)[0])):                       #Check If the directory exist  in output         
                logging.info("Move file from " + file+ " to " + output)         #Log Move action   
                shutil.move(input,output)                                       #Move the file to output
                commitToDB(input,output,"M")                                    #Commiting to Db
                if len(os.listdir(os.path.split(input)[0])) == 0:               #Check if the input directory is empty 
                    os.removedirs(os.path.split(input)[0])                      #Remove directory if empty 
            else:                                                               #If Direcotry doesn't exists in output
                os.makedirs(os.path.split(output)[0])                           #Make the directory 
                logging.info("Move file from " + input+ " to " + output)        #Log the move operation 
                shutil.move(input,output)                                       #Move the file to output 
                commitToDB(input,output,"M")                                    #Commiting to DB
#----------------------------------------------------------------------------------------# 


#-----------Move Directory Method -------------------------------------------------------#
def moveDir(directories):
    for dir in directories:                                                     #For each directory in directories 
        output = r"{}\{}".format(args.o,dir)                                    #append output path to file 
        input = r"{}\{}".format(args.i,dir)                                     #append input path to file
       
        if(not os.path.exists(output)):                                         #If path does not exists 
            logging.info("Create Directory from " + input+ " to " + output)     #log the create directory 
            os.makedirs(output)                                                 #Make the directory at output 
            commitToDB(input,output,("C" if args.C else "M"))                   #Commiting to DB
#----------------------------------------------------------------------------------------#

while 1:                                                                                  #While True 
    time.sleep(10)                                                                        #Let CPU Sleep for a 10 seconds 
    
    # Here we use the same concept of dictionary as we did for output folder 
    # We scan for file and directories in input folder and save it to after check
   
    file_after_check = dict([(f.replace(args.i,""),None) for f in getFilePaths(path_to_watch)])              #Get file paths of files in input folder 
    directory_after_check = dict([(d.replace(args.i,""),None) for d in getDirPaths(path_to_watch)])          #Get Directory path in input folder 
    
    # Now we identify files which are not available in the output folder 
    # Here we check if the output folder has the file which is in input 
    # if not we add them to addedFiles and addedDirectory 
    # The reason why dictionary works is we cannot have any folder which has the same file name 

    addedFiles = [f for f in file_after_check if not f in file_before_check]                  #Check if the filesbeforecheck has the files in aftercheck and if not add them to a list 
    addedDirectory = [d for d in directory_after_check if not d in directory_before_check]    #Check if the directory before check has the directory in after check and if not add them to a list 
    if len(addedDirectory) !=0:                                                               #If the length of new directories are not equal to zero
        logging.info("Following Directory were added" +str(addedDirectory))                   #Log the new directories 
        moveDir(addedDirectory)                                                               #call moveDir method with new directories added 
    if len(addedFiles) !=0:                                                                   #If the length of new files are not equal to zero 
        logging.info("Following files were added "+ str(addedFiles))                          #Log the new files 
        moveFile(addedFiles)                                                                  #Call moveFile method with new files added 
    file_before_check = file_after_check                                                      #make the before check equal to after check file      
    directory_before_check = directory_after_check                                            #make the before check equal to after check directory    
   
   
    
