import re
import numpy as np


def check_basepointgroup_berths_are_at_start_of_seg(cursor):
    seg_berths = []
    bpg_berths = []
    error_bool = "False"
    cursor.execute('''
                    SELECT
                      DISTINCT A.Name,
                      A.HSBerth,
                      A.HSBerth1,
                      A.HSBerth2,
                      A.HSBerth3,
                      A.HSBerth4,
                      A.HSBerth5,
                      A.HSBerth6,
                      A.HSBerth7,
                      A.HSBerth8,
                      A.HSBerth9,
                      A.HSBerth10,
                      A.HSBerth11,
                      A.HSBerth12,
                      A.HSBerth13,
                      A.HSBerth14,
                      A.HSBerth15,
                      B.HSBerth,
                      B.HSBerth1,
                      B.HSBerth2,
                      B.HSBerth3,
                      B.HSBerth4,
                      B.HSBerth5,
                      B.HSBerth6,
                      B.HSBerth7
                    FROM
                      HSInterLocationPathSegment_Data AS A
                      LEFT JOIN HSBasePointsGroup_Data AS B ON A.HSBerth IN (
                        B.HSBerth,
                        B.HSBerth1,
                        B.HSBerth2,
                        B.HSBerth3,
                        B.HSBerth4,
                        B.HSBerth5,
                        B.HSBerth6,
                        B.HSBerth7
                      )''')
    output = cursor.fetchall()
    with open(r"Output\validation_output.txt", "a") as uicFile:
        uicFile.write("***********************\nPath Segments that are missing berths: \n***********************\n")
        for i in output:
            seg_berths = i[:17]
            bpg_berths = i[17:]
            missing_berths = []
            for item in bpg_berths:
                if item != "-":
                    if item not in seg_berths:
                        missing_berths.append(item)
                        error_bool = "True"
            if error_bool == "True":
                uicFile.write("\n {} is missing berths: {} at start of berth list".format(str(i[0]), str(
                    missing_berths)))  # " - BPG berths are: "+str(bpg_berths))
                error_bool = "False"
    uicFile.close()


def check_if_berths_missing_from_bpg(cursor):
    cursor.execute('''
                    SELECT
                      DISTINCT A.Name
                    FROM
                      HSBerth_Data AS A
                      LEFT JOIN HSBasePointsGroup_Data AS B ON A.Name IN (
                        B.HSBerth,
                        B.HSBerth1,
                        B.HSBerth2,
                        B.HSBerth3,
                        B.HSBerth4,
                        B.HSBerth5,
                        B.HSBerth6,
                        B.HSBerth7
                      )
                    WHERE
                      B.HSBerth IS NULL''')
    output = cursor.fetchall()
    with open(r"Output\validation_output.txt", "a") as uicFile:
        uicFile.write("\n\n***********************\nBerths not used in BasePointGroup: \n***********************\n")
        for i in output:
            uicFile.write("\n" + str(i[0]) + " is not in a BasePointGroup. ")
    uicFile.close()


def check_trackcircuits_used(cursor):
    cursor.execute('''
                    SELECT
                        A.Name, B.HSTrackCircuit, C.HSTrackCircuit
                    FROM
                        HSTrackCircuit_Data AS A
                        LEFT JOIN HSRoute_Data AS B ON A.Name IN (
                            B.HSTrackCircuit,
                            B.HSTrackCircuit1,
                            B.HSTrackCircuit2,
                            B.HSTrackCircuit3,
                            B.HSTrackCircuit4,
                            B.HSTrackCircuit5,
                            B.HSTrackCircuit6,
                            B.HSTrackCircuit8,
                            B.HSTrackCircuit9,
                            B.HSTrackCircuit10,
                            B.HSTrackCircuit11,
                            B.HSTrackCircuit12,
                            B.HSTrackCircuit13,
                            B.HSTrackCircuit14,
                            B.HSTrackCircuit15,
                            B.HSTrackCircuit16,
                            B.HSTrackCircuit17,
                            B.HSTrackCircuit18,
                            B.HSTrackCircuit32,
                            B.HSTrackCircuit33,
                            B.HSTrackCircuit34,
                            B.HSTrackCircuit35
                        )
                        LEFT JOIN HSBasePointOfRoute_Data AS C ON A.Name = C.HSTrackCircuit
                    WHERE
                        B.HSTrackCircuit IS NULL AND C.HSTrackCircuit IS NULL''')
    output = cursor.fetchall()
    with open(r"Output\validation_output.txt", "a") as uicFile:
        uicFile.write("\n\n***********************\nTrack circuits not used in Data: \n***********************\n")
        for i in output:
            uicFile.write("\n" + str(i[0]) + " is not used in Berth or BasePointOfRoute files. ")
    uicFile.close()


def retrieve_all_locations(cursor):
    cursor.execute('''
                    SELECT
                        DISTINCT HSLocation, HSLocation1
                    FROM
                        HSInterLocationPathSegment_Data''')
    output = cursor.fetchall()
    locations = []
    for item in output:
        locations.append(item)
    return locations


def check_pathsegment_lengths(cursor):
    # length_threshold signifies the difference in length (m) a segment must be to raise an issue in the output file
    length_threshold = 100

    # Get all unique to/from segments
    locations = retrieve_all_locations(cursor)

    # First for loop retrieves route lists for each unique segment type
    with open(r"Output\validation_output.txt", "a") as uicFile:
        uicFile.write(
            "\n\n***********************\nSegment lengths that indicate possible route length issues: \n***********************\n")
        for z, location in enumerate(locations, 0):
            from_loc, to_loc = str(locations[z][0]), str(locations[z][1])
            cursor.execute('''
                           SELECT
                             HSRoute,
                             HSRoute1,
                             HSRoute2,
                             HSRoute3,
                             HSRoute4,
                             HSRoute5,
                             HSRoute6,
                             HSRoute8,
                             HSRoute9,
                             HSRoute10,
                             HSRoute11,
                             HSRoute12,
                             HSRoute13,
                             HSRoute14,
                             HSRoute15
                           FROM
                             HSInterLocationPathSegment_Data
                           WHERE
                             HSLocation IS '{}'
                             AND HSLocation1 IS '{}';'''.format(from_loc, to_loc))

            output = cursor.fetchall()
            seg_count = len(output)
            segment_lengths = []
            # Check if there are multiple segments for this segment type
            if seg_count > 1:
                # for loop to go through each of the segments returned (only if there are multiple)
                for seg in output:
                    seg_length = 0
                    # for loop to get the route length of each route in the segment
                    for route in seg:
                        # ignore values of "-"
                        if route != "-":
                            cursor.execute('''
                                            SELECT
                                              Int326
                                            FROM
                                              HSRoute_Data
                                            WHERE
                                              Name = '{}';'''.format(route))
                            routelength_output = cursor.fetchone()
                            # add route length to running total for segment
                            try:
                                current_routelength = int(routelength_output[0])
                                seg_length += current_routelength
                            except:
                                print("issue with route length - skipping route..")
                                break

                    # Add finished route length total (segment length) to segment_lengths array
                    segment_lengths.append(seg_length)

                # Caluclations to see if difference in segment lengths constitues concern
                longest_seg = max(segment_lengths)
                shortest_seg = min(segment_lengths)
                length_discrep = int(longest_seg) - int(shortest_seg)
                if length_discrep > length_threshold:
                    uicFile.write("\n" + from_loc + " " + to_loc)
                    uicFile.write("\n The segment lengths between these locations have a discrepancy of: " + str(
                        length_discrep) + "m\n")
    uicFile.close()


def route_length_check(cursor):
    # The following query checks if any locations have their route length set to 999 - which as the default value
    cursor.execute('''
                    SELECT
                      Name,
                      Int326
                    FROM
                      HSRoute_Data''')
    route_lengths = cursor.fetchall()

    with open(r"Output\validation_output.txt", "a") as uicFile:
        uicFile.write("\n\n***********************\nSuspicous route lengths: \n***********************\n")
        for length in route_lengths:
            if length[1] == 999:
                uicFile.write("\n" + str(length[0]) + " has a route length of 999 - please check ")
    uicFile.close()


def loc_boundary_check(cursor):
    # The following query checks if any locations have their EOT value set to 999
    cursor.execute('''
                    SELECT
                      Name,
                      Int329
                    FROM
                      HSLocation_Data
                    WHERE
                      Int329 = 999;''')
    EOT_locations = cursor.fetchall()

    with open(r"Output\validation_output.txt", "a") as uicFile:
        uicFile.write("\n\n***********************\nEOT locations: \n***********************\n")
        for location in EOT_locations:
            if location[1] == 999:
                uicFile.write("\n" + str(location[0]) + " has a EOT value of 999 - please check ")
    uicFile.close()


def check_berth_replace_defined(cursor):
    # The following query finds berths that are declared in HSBerth but not defined in UKBerthStep or UKBerthReplace
    cursor.execute('''
                    SELECT
                      A.Name
                    FROM
                      HSBerth_Data AS A
                      LEFT JOIN UKBerthReplace_Data AS B ON A.Name = B.HSBerth
                      LEFT JOIN UKBerthStep_Data AS C ON A.Name IN (C.HSBerth, C.HSBerth1)
                    WHERE
                      C.HSBerth IS NULL
                      AND (
                        B.bool = 'FALSE'
                        OR B.HSBerth IS NULL
                      );''')
    undefined_berths = cursor.fetchall()

    with open(r"Output\validation_output.txt", "a") as uicFile:
        uicFile.write(
            "\n\n***********************\nBerths undefined in BerthReplace or BerthStep: \n***********************\n")
        for berth in undefined_berths:
            uicFile.write("\n" + str(berth[0]) + " Is defined in HSBerth but not in either BerthReplace or BerthStep ")
    uicFile.close()


def berth_associated_signal_check(cursor):
    cursor.execute('''
                    SELECT
                      HSBasePointOfRoute,
                      HSBasePointOfRoute1,
                      HSBerth,
                      HSBerth1,
                      HSBerth2,
                      HSBerth3,
                      HSBerth4,
                      HSBerth5,
                      HSBerth6,
                      HSBerth7
                    FROM
                      HSBasePointsGroup_Data;''')
    basepoint_groups = cursor.fetchall()

    for bpg in basepoint_groups:
        # Find if BasePointGroup has 2 signals
        sig2 = 'False' if bpg[1] == '-' else 'True'
        list_bpg = list(bpg)
        # Filter out blank values from the list
        bpg_filtered = filter(lambda a: a != '-', list_bpg)
        if sig2 == 'True':
            l_signal, l_berth = bpg_filtered[0], bpg_filtered[2]
            if id_name_match_check(l_signal, l_berth) == 'False':
                print(l_signal + " might not match associated berth: " + l_berth)
            if len(bpg_filtered) > 3:
                r_signal, r_berth = bpg_filtered[1], bpg_filtered[-1]
                if id_name_match_check(r_signal, r_berth) == 'False':
                    print(r_signal + " might not match associated berth: " + r_berth)
        else:
            l_signal, l_berth = bpg_filtered[0], bpg_filtered[1]
            if id_name_match_check(l_signal, l_berth) == 'False':
                print(l_signal + " might not match associated berth: " + l_berth)


def id_name_match_check(id1, id2):
    # Pattern finds signal number in first group
    pattern = R"[RS]\D*0*(\d+)"
    # Pattern2 finds berth number in first group
    pattern2 = R"B..\D*0*(\d+)"

    id_check1 = re.match(pattern, id1)
    if id_check1:
        id_check2 = re.match(pattern2, id2)
        if id_check2:
            # We compare if the signal & berth number extracted match, if not we raise a warning
            if id_check1.group(1) == id_check2.group(1):
                return 'True'
            else:
                return 'False'
    print('Pattern match failed {}  {} '.format(id1, id2))
    return 'Pattern match failed'


'''
**************************************	
Validation Functions still In progress
**************************************		
'''


def find_route_values(route):
    # Check for routes with a route letter - First group matches signal number, second group matches Route letter
    route_regex = re.match(r"R[A-Z]*0*(\d+)[J-VX-Z]*(([A-J])|\W*[MSCW]\W*([A-J]))|(.*)", route)
    # If statement only passes if route has no route letter
    if route_regex.group(1) == None:
        # Search for the signal number if the route has no route letter (1st group matches signal number)
        route_regex = re.match(r"R[A-Z]*0*(\d+)", route)
        # Validation check - if there is no match - return None
        if route_regex == None:
            return [None, None, None, None]
        signal_number = str(route_regex.group(1))
        route_letter = "None"
    else:
        signal_number = str(route_regex.group(1))
        route_letter_seq = str(route_regex.group(2))
        # If the route letter comes after the route type then we need to only select the last returned letter
        route_letter = str(route_letter_seq[-1])

    # The following regex tests if the route is an 'alternate route' (1st group matches alternate route number)
    alternate_route_check = re.match(r"R.+[)-]([1-4])", route)
    if alternate_route_check == None:
        alternate_route = "None"
    else:
        alternate_route = str(alternate_route_check.group(1))

    return [route, signal_number, route_letter, alternate_route]


def route_value_finder_test(cursor):
    cursor.execute("SELECT Name FROM HSRoute_Data WHERE Module = 'TB 1abc'")
    TB_routes = cursor.fetchall()
    checked = set()
    for route in TB_routes:
        route_values = find_route_values(route[0])
        if route_values != None:
            cursor.execute("SELECT Name FROM HSRoute_Data WHERE Module = 'Streatham'")
            all_routes = cursor.fetchall()
            for comparison_route in all_routes:
                comparison_values = find_route_values(comparison_route[0])
            if route_values == comparison_values:
                print(route + "might be the same as" + comparison_route)


def bay_plat_check(cursor):
    '''
    This validation check works by making sure that if a segment is not defined as bay then:
    It has segments going in BOTH directions
    OR
    It's EOT boundary is set to INSYSTEM
    If it doesn't, then it indicates incorrect data
    '''

    # list of bay platforms defined by: segment direction and HSplanning_point EOT value
    bay_fromseg = []
    # list of locations that have segments leaving from either direction
    multi_fromseg = []
    # list of locations that aren't bay - but trains cannot reverse at them
    through_fromseg = []

    # list of bay seg as defined per canpass value in HSPlanning_point
    boundary_frompp = []

    # first query finds every unique locaiton and platform combination
    cursor.execute("SELECT DISTINCT HSLocation, HSPlatformCode FROM HSInterLocationPathSegment_Data")
    unique_loc_plat = cursor.fetchall()

    for z, location in enumerate(unique_loc_plat, 0):
        loc, plat_code = str(unique_loc_plat[z][0]), str(unique_loc_plat[z][1])
        # This test checks if its defined as boundary in HSPlanning_point (in which case we dont care about it)
        boundary_test = check_if_loc_boundary(cursor, loc, plat_code)

        if boundary_test == "False":
            # This query is used to tell if there are segments leaving the platform in different directions
            cursor.execute(
                "SELECT DISTINCT HSLocation, HSDirectionCode, HSPlatformCode FROM HSInterLocationPathSegment_Data "
                "WHERE HSLocation IS '{}' AND HSPlatformCode IS '{}';".format(loc, plat_code))
            seg_directions = cursor.fetchall()

            if len(seg_directions) == 1:
                direction = str(seg_directions[0][1])
                # the following query checks if the platform is a through platform which cant go opposite direction
                cursor.execute(
                    "SELECT DISTINCT HSLocation1, HSPlatformCode1, HSDirectionCode1 FROM HSInterLocationPathSegment_Data WHERE HSLocation1 IS '{}' AND HSPlatformCode1 IS '{}' AND HSDirectionCode1 IS '{}';".format(
                        loc, plat_code, direction))
                through_plat = cursor.fetchall()

                if len(through_plat) == 0:
                    bay_fromseg.append([loc, plat_code])
                else:
                    through_fromseg.append([loc, plat_code])
            else:
                multi_fromseg.append([loc, plat_code])
        else:
            boundary_frompp.append([loc, plat_code])


def check_if_loc_boundary(cursor, loc, plat_code):
    # This query is used to see if a location is at the boundary (and thus SHOULD only have 1 direction of segments)
    cursor.execute(
        "SELECT DISTINCT HSLocation, HSPlatformCode, BoundaryType FROM HSPlanningPoint_Data WHERE HSLocation = '{}' AND HSPlatformCode = '{}';".format(
            loc, plat_code))
    boundary_check1 = cursor.fetchall()
    if boundary_check1[0][2] != 'INSYSTEM':
        return "True"
    else:
        # This query checks if the location is possibly at a boundary by there being no segments to it
        cursor.execute(
            "SELECT DISTINCT HSLocation, HSPlatformCode FROM HSInterLocationPathSegment_Data WHERE HSLocation1 = '{}' AND HSPlatformCode1 = '{}';".format(
                loc, plat_code))
        module_boundary = cursor.fetchall()
        if len(module_boundary) == 0:
            return "True"
        else:
            return "False"
