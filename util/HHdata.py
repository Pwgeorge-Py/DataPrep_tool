import sqlalchemy as db


def produce_hh_data():
    # Connect to database
    engine = db.create_engine(r'sqlite:///DP_tool_v0.1\dp3.db')
    connection = engine.connect()
    metadata = db.MetaData()

    # Get required table details
    HSBerth = db.Table('HSBerth_Data', metadata, autoload=True, autoload_with=engine)
    UKBerthReplace = db.Table('UKBerthReplace_Data', metadata, autoload=True, autoload_with=engine)
    base_groups = db.Table('HSBasePointsGroup_Data', metadata, autoload=True, autoload_with=engine)

    # Produce data
    print("Producing berth & Area ID files...")
    berth_step_and_id_files(connection, HSBerth, UKBerthReplace)
    print("Producing transition berth file...")
    transition_berth_file(connection, base_groups)

    # Close database connection
    connection.close()
    engine.dispose()


def berth_step_and_id_files(connection, HSBerth, UKBerthReplace):
    '''
    Produce AREA ID Table file & Berth Step Table file
   '''

    # Get berths for Berth list and unique area IDs
    query = db.select([HSBerth.c.Name], distinct=True)
    ResultProxy = connection.execute(query)
    all_berths = ResultProxy.fetchall()

    unique_area_ids = set()

    # find dummy berths
    query = db.select([UKBerthReplace.c.HSBerth], distinct=True).where(UKBerthReplace.c.bole == True)
    ResultProxy = connection.execute(query)
    dummy_berths = ResultProxy.fetchall()
    berth_list = []

    # Produce data for areaIdentityTable & berthStepTable from previous outputs
    for z, berth in enumerate(all_berths, 0):
        if berth[0][1:3] not in unique_area_ids:
            unique_area_ids.add(berth[0][1:3])
        if berth not in dummy_berths:
            berth_list.append([berth[0][1:3], berth[0][3:]])

    # Sort output
    unique_area_ids = sorted(unique_area_ids)

    # Write output to file
    with open(r"Output\areaIdentityTable.txt", "a") as areaFile:
        for area_id in unique_area_ids:
            areaFile.write(area_id + "	7\n")
    areaFile.close
    print("HH Berth file created at: Output\areaIdentityTable.txt")

    # Sort output
    berth_list = sorted(berth_list)

    # Write output to file
    with open(r"Output\berthStepTable.txt", "a") as berthFile:
        for berth in berth_list:
            berthFile.write(berth[0] + "	" + berth[1] + "\n")
    berthFile.close()
    print("HH Berth file created at: Output\berthStepTable.txt")


def transition_berth_file(connection, base_groups):
    '''
    Produce Transition Berths data file
    '''
    # Find all berths in basepoint groups
    query = db.select([base_groups.c.HSBerth, base_groups.c.HSBerth1, base_groups.c.HSBerth2, base_groups.c.HSBerth3,
                       base_groups.c.HSBerth4, base_groups.c.HSBerth5, base_groups.c.HSBerth6, base_groups.c.HSBerth7])
    ResultProxy = connection.execute(query)
    berths_base_points = ResultProxy.fetchall()

    # Find all signals in basepoint groups
    query = db.select([base_groups.c.HSBasePointOfRoute, base_groups.c.HSBasePointOfRoute1])
    ResultProxy = connection.execute(query)
    sigs_base_points = ResultProxy.fetchall()

    transitions = 0
    transition_berths = []

    # look through every basepointgroup
    for a, group in enumerate(berths_base_points, 0):

        # check number of signals and extract their identifier
        mult_signals = False
        signal1 = sigs_base_points[a][0]
        signal1id = signal1[-2:]
        signal2 = sigs_base_points[a][1]
        if signal2 != "-":
            mult_signals = True
            signal2id = signal2[-2:]

        # Initialise iterators for each basepoint group:
        # used for finding a transition
        wksid = []
        # Used for checking if berths are similar enough to be a transition
        identifier = []
        # Used to display the berth in the correct output file format
        end_berth = []
        # Used to display in console if transition direction could not be determined
        whole_berths = []

        # Extract required information from each berth
        for berth in group:
            # ignore null values
            if berth != "-":
                if berth is not None:
                    wksid.append(berth[1:3])
                    identifier.append(berth[5:7])
                    end_berth.append(berth[3:7])
                    whole_berths.append(berth)

        # If checks that a transition is present in berth list
        if len(set(wksid)) > 1:
            # compare all elemnts against each other in list (find the dupe!)
            for lposition, left_ID in enumerate(identifier, 0):
                for rposition, right_ID in enumerate(identifier[lposition + 1:], lposition + 1):
                    # Check if last 2 digits of berths match
                    if left_ID == right_ID:
                        # The following checks are to tell which direction the transition travels in
                        if mult_signals == False:
                            # If only 1 signal then transition must go right to left
                            transition_berths.append(
                                wksid[rposition] + "," + end_berth[rposition] + "," + wksid[lposition] + "," +
                                end_berth[lposition])
                        elif lposition != 0:
                            # if first transition berth, not first berth in list, then transition must be left to right
                            transition_berths.append(
                                wksid[lposition] + "," + end_berth[lposition] + "," + wksid[rposition] + "," +
                                end_berth[rposition])
                        elif rposition + 1 == len(whole_berths):
                            # If there are 2 signals but only transition berths, then it must step both ways
                            transition_berths.append(
                                wksid[rposition] + "," + end_berth[rposition] + "," + wksid[lposition] + "," +
                                end_berth[lposition] + "- check stepping allows it to go both ways ")
                            transition_berths.append(
                                wksid[lposition] + "," + end_berth[lposition] + "," + wksid[rposition] + "," +
                                end_berth[rposition] + "- check stepping allows it to go both ways ")
                        elif lposition == 0:
                            # if left berth is first in list then the transition must go right to left
                            transition_berths.append(
                                wksid[rposition] + "," + end_berth[rposition] + "," + wksid[lposition] + "," +
                                end_berth[lposition])
                        else:
                            # If all else fails - transition must be checked manually - input data likely incorrect
                            transition_berths.append(
                                "Search for transition manually -- Signal 1: " + signal1 + "Signal 2: " + signal2 +
                                ", Berths: " + whole_berths[lposition] + ", " + whole_berths[rposition])
                        transitions += 1

    # Sort transition berths
    transition_berths = sorted(transition_berths)

    # Output transition berth to file
    with open(r"Output\transitionsCSV.txt", "a") as transFile:
        for trans in transition_berths:
            transFile.write(trans + "\n")

    # Print number of transitions found
    print(str(transitions) + " transitions found")

    transFile.close()
