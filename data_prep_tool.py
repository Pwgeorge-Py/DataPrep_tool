import os
import sqlite3
import argparse

import util.EXCELtoDB
import util.validateHS
import util.mergeTest
import util.HHdata
import util.TXTtoDB
import util.validateTTS

# Version of Tool
tool_version = 0.1

# Name of database file
database_name = "dp3.db"

# Tool Arguments
parser = argparse.ArgumentParser(
    description='This tool validates and generates Tranista data. Version ' + str(tool_version),
    epilog='Ask Patrick George if you require help')
parser.add_argument('-merged', help='Load 1 set of HS data - (1 module or merged data set) ', action='store_true')
parser.add_argument('-unmerged', help='Load all modules of HS data - (Not yet merged dataset) ', action='store_true')
parser.add_argument('-tts', help='Loads TTS data to database', action='store_true')
parser.add_argument('-validatehs', help='Validates the HS data that is currently loaded', action='store_true')
parser.add_argument('-mergetest', help='Tests for possible issues when merging modules', action='store_true')
parser.add_argument('-validatetts', help='Validates the TTS data that is currently loaded', action='store_true')
parser.add_argument('-hub', help='Produces HH data', action='store_true')

args = parser.parse_args()


def clear_output_files(file):
    if os.path.exists(file):
        print("Removing " + file)
        os.remove(file)


def connect_to_db():
    # Open database
    db = sqlite3.connect(database_name)
    cursor = db.cursor()
    return cursor, db


# **************************************************
# Tool arguments
# **************************************************
if args.merged:
    # Delete existing DB
    clear_output_files(database_name)
    # Delete existing output file of column names
    clear_output_files("Output\col_names.txt")
    # Connect to DB
    cursor, db = connect_to_db()
    # Load 1 module into database
    util.EXCELtoDB.load_all(cursor, db)
    # Close DB
    db.close()

if args.unmerged:
    # Delete existing DB
    clear_output_files(database_name)
    # Delete existing output file of column names
    clear_output_files("Output\col_names.txt")
    # Connect to DB
    cursor, db = connect_to_db()
    # Load all modules into database
    util.EXCELtoDB.load_all_unmerged(cursor, db)
    # Close DB
    db.close()

if args.mergetest:
    # Delete existing output file of column names
    clear_output_files(r"Output\merge_test.txt")
    # Connect to DB
    cursor, db = connect_to_db()
    # Test data set
    util.mergeTest.test_data_merge(cursor)
    # Close DB
    db.close()

if args.validatehs:
    # Delete existing validation output file
    clear_output_files(r"Output\validation_output.txt")
    # Connect to DB
    cursor, db = connect_to_db()
    # Run validation functions from validateHS module:
    util.validateHS.check_basepointgroup_berths_are_at_start_of_seg(cursor)
    util.validateHS.check_if_berths_missing_from_bpg(cursor)
    util.validateHS.check_trackcircuits_used(cursor)
    util.validateHS.check_pathsegment_lengths(cursor)
    util.validateHS.route_length_check(cursor)
    util.validateHS.loc_boundary_check(cursor)
    util.validateHS.check_berth_replace_defined(cursor)
    # Close DB
    db.close()

if args.tts:
    # Connect to DB
    cursor, db = connect_to_db()
    # Load TTS data to database
    util.TXTtoDB.load_drle(db)
    util.TXTtoDB.load_dins(db)
    util.TXTtoDB.load_dloc(db)
    # Close DB
    db.close()

if args.validatetts:
    # Delete existing validation output file
    clear_output_files(r"Output\tts_validation_output.txt")
    # Connect to DB
    cursor, db = connect_to_db()
    # Run validation functions from validateTTS module:
    util.validateTTS.unique_ID_check(cursor)
    util.validateTTS.fct_mismatch(cursor)
    util.validateTTS.dloc_locations_in_hs(cursor)
    util.validateTTS.dummy_line_in_dins(cursor)
    util.validateTTS.dummy_line_inserted(cursor)
    # Close DB
    db.close()

if args.hub:
    # Delete existing HH output files
    clear_output_files(r"Output\areaIdentityTable.txt")
    clear_output_files(r"Output\berthStepTable.txt")
    clear_output_files(r"Output\transitionsCSV.txt")
    # Create HH data
    util.HHdata.produce_hh_data()


