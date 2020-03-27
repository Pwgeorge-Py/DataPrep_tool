import re
import pandas as pd


def load_drle(db):
    drle_columns = ["ID", "INL", "INF", "INC", "NPC", "REL", "REF", "REC"]

    # Regex used to count the number of DRLES in the file
    drle_count_regex = r"INL="

    # Regex used to count how many of the DRLEs are commented out
    commented_drle_count_regex = r"/.+INL="

    '''
    (drle_find_regex) Regex used to find the DRLEs in text file, Groups:
    1)unique ID
    2)INL
    3)INF
    4)INC
    5)NPC
    6)REL
    7)REF
    8)REC
    '''
    drle_find_regex = r"\n([KI]\w*)\s+INL=\s*([\w]+);.*\n.*INF=([FPL]);.*\n.*INC=([\w]*);.*\n.*" \
                      r"NPC=([PCN]);.*\n.*REL=([\w]+);.*\n.*REF=([FPL]*);.*\n.*REC=([\w\*]*)\."

    with open(r"./Input/TTS/TM10_DRLE", "r") as ttsFile:
        # Convert file to text
        text = ttsFile.read()

        # Count number of DRLEs in file
        total_drles = len(re.findall(drle_count_regex, text))
        print("Number of DRLEs in file: {}".format(str(total_drles)))

        # Count number of DRLEs that are commented out in file
        commented_drles = len(re.findall(commented_drle_count_regex, text))
        print("Number of DRLEs commented out in file: {}".format(str(commented_drles)))

        # Create list of all matches
        drles = re.findall(drle_find_regex, text)

        # Count number of DRLEs that were caught
        caught_drles = len(drles)
        print("Number of DRLEs caught: {}".format(str(caught_drles)))

        # Check all possible DRLES were caught by regex
        if commented_drles + caught_drles < total_drles:
            print("NOT ALL DRLES CAUGHT - check SYNTAX of input file")
            print("Total number of DRLES detected: {} - Total number of DRLES caught {}".format(str(total_drles), str(
                commented_drles + caught_drles)))

        # Add DRLES to database
        drle_frame = pd.DataFrame(data=drles, columns=drle_columns)
        drle_frame.to_sql("DRLE", db, index=False, if_exists="replace")
    ttsFile.close()


def load_dins(db):
    dins_columns = ["ID", "STL", "STF", "STC", "ENL", "ENF", "ENC", "INL", "FCT"]
    bad_dins_columns = ["ID", "STL", "STF", "STC", "INL", "ENL", "ENF", "ENC", "FCT"]

    # Regex used to count the number of DINS in the file
    dins_count_regex = r"STL="

    # Regex used to count how many of the DINS are commented out
    commented_dins_count_regex = r"/.+STL="

    '''
    (dins_find_regex1) Regex used to find wrong ordered DINS - then restructured in pandas to match dins_find_regex2 structure
    (dins_find_regex2) Regex used to find the DINS in text file, Groups:
    1)unique ID
    2)STL
    3)STF
    4)STC
    5)ENL
    6)ENF
    7)ENC
    8)INL
    9)FCT
    '''
    dins_find_regex1 = r"\n([KI]\w*)\s+STL=\s*([\w]+);.*\n.*\n.*STF=\s*(\w*);.*\n.*STC=\s*(\w*);.*\n.*\n.*INL=\s*" \
                       r"(\w*);.*\n.*ENL=\s*(\w*);.*\n.*\n.*ENF=\s*(\w*);.*\n.*ENC=\s*(\w*);.*\n.*\n.*FCT=\s*(\w*)."
    dins_find_regex2 = r"\n([KI]\w*)\s+STL=\s*([\w]+);.*\n.*\n.*STF=\s*(\w*);.*\n.*STC=\s*(\w*);.*\n.*\n.*ENL=\s*" \
                       r"(\w*);.*\n.*\n.*ENF=\s*(\w*);.*\n.*ENC=\s*(\w*);.*\n.*\n.*INL=\s*(\w*);.*\n.*FCT=\s*(\w*)."

    with open(r"./Input/TTS/TM10_DINS", "r") as ttsFile:
        # Convert file to text
        text = ttsFile.read()

        # Count number of DINS in file
        total_dins = len(re.findall(dins_count_regex, text))
        print("Number of DINS in file: {}".format(str(total_dins)))

        # Count number of DINS that are commented out in file
        commented_dins = len(re.findall(commented_dins_count_regex, text))
        print("Number of DINS commented out in file: {}".format(str(commented_dins)))

        # Create list of all incorrectly ordered matches
        bad_dins = re.findall(dins_find_regex1, text)
        # Add incorrectly ordered DINS to pandas dataframe
        bad_dins_frame = pd.DataFrame(data=bad_dins, columns=bad_dins_columns)

        # Create list of all remaining DINS
        dins = re.findall(dins_find_regex2, text)
        # Add DINS to a dataframe
        dins_frame = pd.DataFrame(data=dins, columns=dins_columns)

        # Count number of DINS that were caught
        caught_dins = len(dins) + len(bad_dins)
        print("Number of DINS caught: {}".format(str(caught_dins)))

        # Check all possible DINS were caught by regex
        if commented_dins + caught_dins < total_dins:
            print("NOT ALL DINS CAUGHT - check SYNTAX of input file")
            print("Total number of DINS detected: {} - Total number of DINS caught {}".format(str(total_dins), str(
                commented_dins + caught_dins)))

        # Merge 2 frames
        dins_frame = pd.concat([dins_frame, bad_dins_frame], ignore_index=True, sort=True)
        # Add DINS to database
        dins_frame.to_sql("DINS", db, index=False, if_exists="replace")
    ttsFile.close()


def load_dloc(db):
    # Regex used to count the number of DLOCs in the file
    dloc_count_regex = "PLC="
    # Regex used to count how many of the DLOCs are commented out
    commented_dloc_count_regex = r"\n/(\w+)\s*PLC=.*"
    # Regex used to catch all wanted DLOCs
    dloc_find_regex = r"\n(\w+)\s*PLC=.*"

    with open(r"./Input/TTS/TM10_DLOC", "r") as ttsFile:
        # Convert file to text
        text = ttsFile.read()

        # Count number of DLOCs in file
        total_dloc = len(re.findall(dloc_count_regex, text))
        print("Number of DLOCs in file: {}".format(str(total_dloc)))

        # Count number of DLOCs that are commented out in file
        commented_dloc = len(re.findall(commented_dloc_count_regex, text))
        print("Number of DLOCs commented out in file: {}".format(str(commented_dloc)))

        # Create list of all matches
        dlocs = re.findall(dloc_find_regex, text)

        # Count number of DLOCs that were caught
        caught_dlocs = len(dlocs)
        print("Number of DLOCs caught: {}".format(str(caught_dlocs)))

        # Check all possible DLOCs were caught by regex
        if commented_dloc + caught_dlocs < total_dloc:
            print("NOT ALL DLOCs CAUGHT - check SYNTAX of input file")
            print("Total number of DLOCs detected: {} - Total number of DLOCs caught {}".format(str(total_dloc), str(
                commented_dloc + caught_dlocs)))

        # Add DLOC to database
        dloc_series = pd.Series(data=dlocs, name="Location")
        dloc_series.to_sql("DLOC", db, index=False, if_exists="replace")
    ttsFile.close()
