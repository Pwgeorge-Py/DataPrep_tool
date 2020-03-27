final_start_list = []


def unique_ID_check(cursor):
    # The following query checks for duplicated ID column values in DRLE table
    cursor.execute('''
                    SELECT
                      ID,
                      COUNT(*)
                    FROM
                      DRLE
                    GROUP BY
                        ID
                    HAVING
                        COUNT(*) > 1;''')
    drle_id_dupes = cursor.fetchall()

    # The following query checks for duplicated ID column values in DINS table
    cursor.execute('''
                    SELECT
                      ID,
                      COUNT(*)
                    FROM
                      DINS
                    GROUP BY
                        ID
                    HAVING
                        COUNT(*) > 1;''')
    dins_id_dupes = cursor.fetchall()

    with open(r"Output\tts_validation_output.txt", "a") as ttsFile:
        ttsFile.write("\n\n***********************\nDuplicated IDs in DRLE file: \n***********************\n")
        for dupe in drle_id_dupes:
            ttsFile.write("\n DRLE ID: {} is used {} times.".format(str(dupe[0]), str(dupe[1])))

        ttsFile.write("\n\n***********************\nDuplicated IDs in DINS file: \n***********************\n")
        for dupe in dins_id_dupes:
            ttsFile.write("\n DINS ID: {} is used {} times.".format(str(dupe[0]), str(dupe[1])))

    ttsFile.close()


def fct_mismatch(cursor):
    # Query to find duplicated DINS with differing ID values
    cursor.execute('''
                   SELECT
                     ID, STL, STF, STC, ENL, ENF, ENC, INL, FCT,
                     COUNT(distinct FCT)
                   FROM
                     DINS
                   GROUP BY
                     STL, STF, STC, ENL, ENF, ENC, INL
                   HAVING
                     COUNT(distinct FCT) > 1;''')
    duped_dins = cursor.fetchall()

    with open(r"Output\tts_validation_output.txt", "a") as ttsFile:
        ttsFile.write("\n\n***********************\nMismatched FCT values in DINS file: \n***********************\n")
        for dupe in duped_dins:
            ttsFile.write(
                "\n DINS ID: {} has different FCT value to another DINS rule with the exact same conditions, which is correct?".format(
                    str(dupe[0])))

    ttsFile.close()


def dloc_locations_in_hs(cursor):
    # Query checks if any locations have been duplicated in the DLOC file
    cursor.execute('''
                   SELECT
                     Location,
                     COUNT(Location)
                   FROM
                     DLOC
                   GROUP BY
                     Location
                   HAVING
                     COUNT(Location) > 1;''')
    duped_dloc = cursor.fetchall()

    # This query checks locations in DLOC that aren't used in HS data
    cursor.execute('''
                   SELECT
                     DISTINCT A.Location,
                     B.HSLocation
                   FROM
                     DLOC AS A
                     LEFT JOIN HSPlanningPoint_Data AS B ON A.Location = B.HSLocation
                   WHERE
                     B.HSLocation IS NULL;''')
    missing_in_hs = cursor.fetchall()

    # This query checks locations in HS that arent in the DLOC
    cursor.execute('''
                   SELECT
                     DISTINCT B.HSLocation,
                     A.Location
                   FROM
                     HSPlanningPoint_Data AS B
                     LEFT JOIN DLOC AS A ON B.HSLocation = A.Location
                   WHERE
                     A.Location IS NULL;''')
    missing_in_dloc = cursor.fetchall()

    # Print results to output file
    with open(r"Output\tts_validation_output.txt", "a") as ttsFile:
        ttsFile.write("\n\n***********************\nDLOC location issues: \n***********************\n")
        for dupe in duped_dloc:
            ttsFile.write(f"\n Location: {dupe[0]} has been duplicated in the DLOC file")
        for hsloc in missing_in_hs:
            ttsFile.write(f"\n Location: {hsloc[0]} Is present in DLOC file but has no HSPlanningPoint")
        for dloc in missing_in_dloc:
            ttsFile.write(f"\n Location: {dloc[0]} Is present in HSPlanningPoint file but is not present in DLOC")

    ttsFile.close()


def dummy_line_in_dins(cursor):
    # This function checks if a dummy line code is used in the DINS
    cursor.execute('''
                    SELECT
                      DISTINCT A.Int32, B.INL, A.Name
                    FROM
                      HSLineCode_Data AS A
                      LEFT JOIN DINS AS B ON A.Name IN (
                        B.STC,
                        B.ENC
                      )
                    WHERE
                      B.INL IS NOT NULL;''')
    dummy_dins = cursor.fetchall()
    # print(dummy_dins)
    with open(r"Output\tts_validation_output.txt", "a") as ttsFile:
        ttsFile.write("\n\n***********************\nDummy line/path codes used in DINS: \n***********************\n")
        for item in dummy_dins:
            if item[0] > 60000:
                ttsFile.write(f"\n Dummy line code {item[2]} used when inserting location: {item[1]}")
    ttsFile.close()


def dummy_line_inserted(cursor):
    # This function checks that all dummy line codes are being inserted somewhere
    cursor.execute('''
                    SELECT
                        DISTINCT A.Name,
                        A.Int32,
                        B.INC
                    FROM
                        HSLineCode_Data AS A
                        LEFT JOIN DRLE AS B ON A.Name = B.INC
                    WHERE
                        B.INC IS NULL;''')
    codes_not_used = cursor.fetchall()
    print(codes_not_used)
    with open(r"Output\tts_validation_output.txt", "a") as ttsFile:
        ttsFile.write("\n\n***********************\nDummy line/path codes used in DINS: \n***********************\n")
        for item in codes_not_used:
            if int(item[1]) > 60000:
                ttsFile.write(f"\n Dummy line code {item[0]} is not being inserted in DRLES")

    ttsFile.close()


# class DinsCheck:

# def __init__(self, location, basepoints, pp_groups):
# self.basepoints, self.pp_groups = get_basepoint_groups(location, cursor)


# @staticmethod
def get_basepoint_groups(location, cursor):
    cursor.execute(f'''
                    SELECT
                      DISTINCT Name, HSBerth
                    FROM
                      HSPlanningPoint_Data
                    WHERE
                      HSLocation = '{location}';''')
    planning_points = cursor.fetchall()

    basepoints = []
    pp_groups = []

    for pp in planning_points:
        if any(pp[1] in sl for sl in basepoints) is False:
            cursor.execute(f'''
                            SELECT
                              HSBerth, 
                              HSBerth1, 
                              HSBerth2, 
                              HSBerth3, 
                              HSBerth4, 
                              HSBerth5, 
                              HSBerth6, 
                              HSBerth7
                            FROM
                              HSBasePointsGroup_Data
                            WHERE '{pp[1]}' IN (
                              HSBerth, 
                              HSBerth1, 
                              HSBerth2, 
                              HSBerth3, 
                              HSBerth4, 
                              HSBerth5, 
                              HSBerth6, 
                              HSBerth7
                            );''')
            bpg_berths = cursor.fetchone()

            tmp = []
            for item in list(bpg_berths):
                tmp.append(item)

            basepoints.append(tmp)
            pp_groups.append([pp[0]])

        else:
            for c, sublist in enumerate(basepoints, 0):
                if pp[1] in sublist:
                    pp_groups[c].append(pp[0])

    return pp_groups


def non_mand_check(cursor):
    cursor.execute(f'''
                    SELECT
                      DISTINCT INL
                    FROM
                      DINS;''')
    Non_mand_locs = cursor.fetchall()


def find_end_locs_from_pp(plan_point, direction, cursor):
    location = plan_point[:plan_point.find("_")]
    basepoints, pp_groups = get_basepoint_groups(location, cursor)


# --------------------------------------------------------------------------


def non_mand_check(cursor):
    cursor.execute('''
                   SELECT
                     DISTINCT INL
                   FROM
                     DINS;''')
    Non_mand_locs = cursor.fetchall()

    find_prev_from_pp(Non_mand_locs, "BCKNHMJ_BVC0169", cursor)

def get_planning_points(location, cursor):
    cursor.execute(f'''
                    SELECT
                      DISTINCT Name, HSBerth
                    FROM
                      HSPlanningPoint_Data
                    WHERE
                      HSLocation = '{location}';''')
    planning_points = cursor.fetchall()


def find_prev_from_pp(Non_mand_locs, planning_point, cursor, start_locs=[]):
    list_of_start_locs = []
    start_loc_list = []

    cursor.execute(f'''
                    SELECT
                      DISTINCT HSPlanningPoint
                    FROM
                      HSInterLocationPathSegment_Data
                    WHERE
                      HSPlanningPoint1 = '{planning_point}';''')
    prev_pps = cursor.fetchall()

    # Add the initial item to the list
    if len(start_locs) == 0:
        start_locs.append(planning_point)

    if len(prev_pps) < 1:
        print("bgp other pp check hit")
        '''
        This is here to validate when a previous PP cant be found (likely a different one being used - usually at a transition)
        search for berths in the same basepoint group of the PP, then search for segments to that PP
        but a check must be added to make sure they are going the same direction
        '''

        # find the current planning points direction
        current_dir = find_pp_direction_start(planning_point, cursor)

        # get location from pp
        location = planning_point[:planning_point.find("_")]

        # retrieve planning points that are in the same basepointgroup
        pp_groups = get_basepoint_groups(location, cursor)
        for group in pp_groups:
            # print(group)
            if planning_point in group:
                for pp in group:
                    # print(pp, planning_point)
                    if pp != planning_point:

                        # check if a planning point is the same direction - if so look back from that planning point
                        pp_dir = find_pp_direction_start(pp, cursor)
                        if pp_dir == current_dir:
                            cursor.execute(f'''
                                            SELECT
                                              DISTINCT HSPlanningPoint
                                            FROM
                                              HSInterLocationPathSegment_Data
                                            WHERE
                                              HSPlanningPoint1 = '{pp}';''')
                            prev_pps = cursor.fetchall()
                            if start_locs[-1] == planning_point:
                                print("berth skewed at" + planning_point + pp)
                                start_locs[-1] = pp
                            break

    path_end = False
    printit = False
    for pp_group in prev_pps:
        for pp in pp_group:
            loc = pp[:pp.find("_")]
            if any(loc in sl for sl in Non_mand_locs) is False:
                final_loc_list = start_locs.copy()
                final_loc_list.append(pp)
                start_loc_list.append([final_loc_list])
                path_end = True
            else:
                path_end = False
                branch_start_locs = start_locs.copy()
                branch_start_locs.append(pp)
                find_prev_from_pp(Non_mand_locs, pp, cursor, branch_start_locs)
            if path_end is True:
                list_of_start_locs.append(final_loc_list)
                printit = True
    if printit == True:
        for item in list_of_start_locs:
            # print(item)
            final_start_list.append(item)
        print(final_start_list)


def find_pp_direction_start(planning_point, cursor):
    print("non-match!" + planning_point)
    cursor.execute(f'''
                    SELECT
                      DISTINCT HSDirectionCode
                    FROM
                      HSInterLocationPathSegment_Data
                    WHERE
                      HSPlanningPoint = '{planning_point}'
                    OR
                      HSPlanningPoint1 = '{planning_point}';''')
    pp_dir = cursor.fetchone()
    print(pp_dir)
    return pp_dir


def check_dins(lists_of_start_locs, lists_of_end_locs, cursor):
    """
    Required input format = [[x, y, g], [z], [p]] (lists of lists)
    The original non-mand location needs to be added to the end of the start_loc_group
    or the start of the end_loc_group to function correctly

    """
    for start_loc_group in lists_of_start_locs:
        for end_loc_group in lists_of_end_locs:

            all_loc_group = start_loc_group + end_loc_group
            print(all_loc_group)

            dins_found = 0

            # Another for loop to make sure DINS rules are found sequentially (so it doesnt pick up opposite direction)
            # The indices of the element selected from ENL must always be higher then that of the STL.
            for c, start_loc in enumerate(all_loc_group[:-2], 1):
                end_loc_string = "', '".join(all_loc_group[c + 1:])
                non_mand_loc_string = "', '".join(all_loc_group[c:-1])

                cursor.execute('''
                               SELECT
                                 *
                               FROM
                                 DINS
                               WHERE
                                 STL = {}
                                 AND ENL IN ({})
                                 AND INL IN ({})
                               '''.format("'" + start_loc + "'", "'" + end_loc_string + "'",
                                       "'" + non_mand_loc_string + "'"))
                output = cursor.fetchall()

                dins_found += len(output)
                print("iteration: " + str(c))
                print(start_loc)
                print(non_mand_loc_string)
                print(end_loc_string)

            # Number of DINS required is always the sum of all numbers up to the number of non mandatory locations
            # We "-2" the length of the list as the first and last values must be mandatory locations
            dins_required = 0
            for i in range(1, len(all_loc_group) - 1):
                dins_required += i
            print(dins_required)

            # If the number of DINS found is less than whats required - print an error
            if dins_found < dins_required:
                print("somethings wrong here...")
                print(dins_required)
                print(dins_found)
