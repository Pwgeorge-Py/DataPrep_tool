def test_data_merge(cursor):
    table_names = ["HSInterLocationPathSegment_Data",
                   "HSPlanningPoint_Data",
                   "HSBasePointOfRoute_Data",
                   "HSBasePointsGroup_Data",
                   "HSPlatform_Data",
                   "HSRoute_Data",
                   "HSTrackCircuit_Data",
                   "HSLocation_Data",
                   "UKBerthReplace_Data",
                   "UKBerthStep_Data"]

    with open(r"Output\merge_test.txt", "a") as uicFile:
        uicFile.write("\n\n***********************\nID's that have been duplicated: \n***********************\n")
        for c, table in enumerate(table_names, 0):
            uicFile.write("\n\n***********************\n" + table + " \n***********************\n")
            columns = get_col_names(table, cursor)
            # Set used to not check same value twice
            checked = set()
            cursor.execute("SELECT Name FROM " + table)
            output = cursor.fetchall()
            for item in output:
                if item not in checked:
                    checked.add(item)
                    # Query used to check if the name is used multiple times in the table
                    cursor.execute("SELECT Name FROM " + table + " WHERE Name = '" + item[0] + "';")
                    name_match = cursor.fetchall()
                    if len(name_match) > 1:
                        # Query used to tell if the duplicated named rows have any other different values
                        cursor.execute(
                            "SELECT DISTINCT [" + ("], [".join(columns[1:-1])) + "] FROM " + table + " WHERE Name = '" + name_match[0][0] + "';")
                        dupe_match = cursor.fetchall()
                        if len(dupe_match) > 1:
                            cursor.execute("SELECT Name, Module FROM " + table + " WHERE Name = '" + item[0] + "';")
                            dupe_module = cursor.fetchall()
                            for item in dupe_module:
                                uicFile.write(str(item[0]) + " , in Module: " + str(item[1]) +
                                              " - Has multiple instances but different attributes, which is correct?\n")


def get_col_names(table_name, cursor):  # Extracting column names from database
    cursor.execute("SELECT * FROM {}".format(table_name))
    columns = list(map(lambda x: x[0], cursor.description))
    return columns
