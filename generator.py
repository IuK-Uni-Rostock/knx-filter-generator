import click
import pymssql
from pick import pick


@click.command()
@click.option('--dbhost', prompt='Database Host', default='localhost')
@click.option('--dbuser', prompt='Database User', default='SA')
@click.option('--dbpass', prompt='Database Password', hide_input=True, default='<YourStrong!Passw0rd>')
@click.option('--dbname', prompt='Database Name', default='KNX')
@click.option('--filename', prompt='Filename', default='rules.txt')
def cli(dbhost, dbuser, dbpass, dbname, filename):
    click.clear()
    click.echo('Connecting ...')
    conn = get_db_connection(dbhost, dbuser, dbpass, dbname)
    click.clear()

    projects = get_all_projects(conn)
    title = 'Please select a project:'
    selected = pick(projects, title, indicator='>', options_map_func=get_label)

    project_id = selected[0]['id']
    installation_id = get_installation_id_from_project(conn, project_id)
    couplers = get_all_couplers_for_installation_id(conn, installation_id)

    title = 'Please select one or more coupler:'
    selected_couplers = pick(couplers, title, options_map_func=get_label, multi_select=True, min_selection_count=1)

    f = open(filename, "w+")
    for coupler in selected_couplers:
        f.write('# Rules for %d.%d.0\r\n' % (coupler[0]["Area.Address"], coupler[0]["Line.Address"]))
        rules = generate_rules_for_coupler(conn, installation_id, coupler[0]["Area.Address"], coupler[0]["Line.Address"])

        f.write('## Egress rules\r\n')
        for r in rules[0]:
            f.write(r + "\r\n")

        f.write('\r\n## Ingress rules\r\n')
        for r in rules[1]:
            f.write(r + "\r\n")

        f.write("\r\n")


def get_db_connection(dbhost, dbuser, dbpass, dbname):
    return pymssql.connect(dbhost, dbuser, dbpass, dbname)


def get_all_projects(conn):
    cursor = conn.cursor(as_dict=True)
    cursor.execute('SELECT ID, Name FROM [dbo].[Project]')

    results = []
    for row in cursor:
        results.append({"id": row['ID'], "label": row['Name']})

    return results


def get_installation_id_from_project(conn, project_id):
    cursor = conn.cursor(as_dict=True)
    cursor.execute('SELECT [ID] FROM [dbo].[Installation] WHERE [ProjectID] = %s', project_id)
    row = cursor.fetchone()
    return row['ID']


def get_all_couplers_for_installation_id(conn, installation_id):
    cursor = conn.cursor(as_dict=True)
    cursor.execute("""SELECT
        Device.ID AS 'Device.ID',
        Device.Address AS 'DeviceAddress',
        Device.InstallationID AS 'Device.InstallationID',
        Device.Description as 'Device.Description',
        Area.Address AS 'Area.Address',
        Line.Address AS 'Line.Address'
  FROM [Device]
        JOIN Line ON (Device.LineID = Line.ID)
        JOIN Area ON (Line.AreaID = Area.ID)
  WHERE [Device].[Address] = 0 AND Area.InstallationID = %s
  ORDER BY
        [Area.Address] ASC,
        [Line.Address] ASC""", installation_id)

    results = []
    for row in cursor:
        row["label"] = '{}.{}.{}'.format(row["Area.Address"], row["Line.Address"], 0)
        results.append(row)

    return results


def generate_rules_for_coupler(conn, installation_id, area, line):
    egress = []
    ingress = []

    if line == 0:
        group_addresses = get_group_addresses_for_area(conn, installation_id, area)
    else:
        group_addresses = get_group_addresses_for_line(conn, installation_id, area, line)

    for group_address_id in group_addresses:
        group_address_type = get_group_address_type_for_group_address(conn, group_address_id)
        if (group_address_type["DatapointType"] is not None) and (group_address_type["DatapointType"] != ""):
            group_address_value = group_address_type["DatapointType"]
            group_address_type = "DPT"
        elif (group_address_type["ObjectSize"] is not None) and (group_address_type["ObjectSize"] != ""):
            group_address_value = group_address_type["ObjectSize"]
            group_address_type = "ObjSize"
        else:
            print("Can't extract data point type for group-address: %s" % group_address_id)
            continue

        if group_address_type == 'DPT':
            group_address_filter_comp = "--dpt %s " % group_address_value
        else:
            group_address_filter_comp = "--ObjSize %s " % group_address_value

        devices = get_all_devices_related_to_group_address(conn, installation_id, group_address_id)
        # A device can have the multiple device objects referring to the same communication object
        # In order to prevent multiple Rules for the same Src address + group address, we need to
        # merge entries with identical AreaAddress, LineAddress, DeviceAddress, GroupAddressNo
        # and take the biggest set of permissions. One device with TransmitFlag True, the other with false, the merger
        # will be set to true. Same for other properties.
        # For Priority the highest priority has to be considered.
        # Hence the matching will not match the exact priority, but only consider it a max priority.

        devices = merge_duplicate_devices(devices)

        internal_devices = filter_devices_inside_coupler(devices, area, line)
        external_devices = filter_devices_outside_couplers(devices, area, line)

        internal_read_all = filter_devices_read_flag(internal_devices)
        internal_read_send_only = filter_sending(filter_devices_read_flag(internal_devices))
        internal_transmit_send_only = filter_sending(filter_devices_transmit_flag(internal_devices))
        internal_update_all = filter_devices_update_flag(internal_devices)
        internal_write_all = filter_devices_write_flag(internal_devices)
        external_read_all = filter_devices_read_flag(external_devices)
        external_read_send_only = filter_sending(filter_devices_read_flag(external_devices))
        external_transmit_send_only = filter_sending(filter_devices_transmit_flag(external_devices))
        external_update_all = filter_devices_update_flag(external_devices)
        external_write_all = filter_devices_write_flag(external_devices)

        if len(internal_write_all) > 0:
            for d in external_transmit_send_only:
                rule = "FORWARD "
                rule += "%s GRP %s " % (format_physical_address(d), d["ReadableGroupAddress"])
                rule += "--frameType STANDARD "
                rule += "--service A_GroupValue_Write "
                rule += "--priority %s " % (d["Priority"])
                rule += "--hopCount 6 "
                rule += group_address_filter_comp

                ingress.append(rule)

        for d in internal_transmit_send_only:
            if len(external_write_all) > 0:
                rule = "FORWARD "
                rule += "%s GRP %s " % (format_physical_address(d), d["ReadableGroupAddress"])
                rule += "--frameType STANDARD "
                rule += "--service A_GroupValue_Write "
                rule += "--priority %s " % (d["Priority"])
                rule += "--hopCount 6 "
                rule += group_address_filter_comp

                egress.append(rule)

            else:
                rule = "DROP "
                rule += "%s GRP %s " % (format_physical_address(d), d["ReadableGroupAddress"])
                rule += "--frameType STANDARD "
                rule += "--service A_GroupValue_Write "
                rule += "--priority %s " % (d["Priority"])
                rule += "--hopCount 6 "
                rule += group_address_filter_comp

                egress.append(rule)

        if len(internal_update_all) > 0:
            for d in external_read_send_only:
                rule = "FORWARD "
                rule += "%s GRP %s " % (format_physical_address(d), d["ReadableGroupAddress"])
                rule += "--frameType STANDARD "
                rule += "--service A_GroupValue_Response "
                rule += "--priority %s " % (d["Priority"])
                rule += "--hopCount 6 "
                rule += group_address_filter_comp
                rule += "--establishedOnly"

                ingress.append(rule)

        if len(internal_read_all):
            for d in external_update_all:
                rule = "FORWARD "
                rule += "%s GRP %s " % (format_physical_address(d), d["ReadableGroupAddress"])
                rule += "--frameType STANDARD "
                rule += "--service A_GroupValue_Read "
                rule += "--priority %s " % (d["Priority"])
                rule += "--hopCount 6 "
                rule += "--data ^00000$"

                ingress.append(rule)

        for d in internal_update_all:
            if len(external_read_all) > 0:
                rule = "FORWARD "
                rule += "%s GRP %s " % (format_physical_address(d), d["ReadableGroupAddress"])
                rule += "--frameType STANDARD "
                rule += "--service A_GroupValue_Read "
                rule += "--priority %s " % (d["Priority"])
                rule += "--hopCount 6 "
                rule += "--data ^00000$"

                egress.append(rule)

            elif len(internal_read_all) > 0:
                rule = "DROP "
                rule += "%s GRP %s " % (format_physical_address(d), d["ReadableGroupAddress"])
                rule += "--frameType STANDARD "
                rule += "--service A_GroupValue_Read "
                rule += "--priority %s " % (d["Priority"])
                rule += "--hopCount 6 "
                rule += "--data ^00000$"

                egress.append(rule)

        for d in internal_read_send_only:
            if len(external_update_all) > 0:
                rule = "FORWARD "
                rule += "%s GRP %s " % (format_physical_address(d), d["ReadableGroupAddress"])
                rule += "--frameType STANDARD "
                rule += "--service A_GroupValue_Response "
                rule += "--priority %s " % (d["Priority"])
                rule += "--hopCount 6 "
                rule += group_address_filter_comp
                rule += "--establishedOnly"

                egress.append(rule)

            elif len(internal_update_all) > 0:
                rule = "DROP "
                rule += "%s GRP %s " % (format_physical_address(d), d["ReadableGroupAddress"])
                rule += "--frameType STANDARD "
                rule += "--service A_GroupValue_Response "
                rule += "--priority %s " % (d["Priority"])
                rule += "--hopCount 6 "
                rule += group_address_filter_comp
                rule += "--establishedOnly"

                egress.append(rule)

    ingress.append("DROP any any any")
    egress.append("NOISE any any any --log")
    return egress, ingress


def get_group_addresses_for_area(conn, installation_id, area):
    cursor = conn.cursor(as_dict=True)
    cursor.execute("""SELECT DISTINCT GroupAddressID
  FROM
     [Connector]
      JOIN Device on (Connector.DeviceID = Device.ID)
      JOIN Line ON (Device.LineID = Line.ID)
      JOIN Area ON (Line.AreaID = Area.ID)
 WHERE
      Device.InstallationID = %s AND
      Area.Address = %d""", (installation_id, area))

    results = []
    for row in cursor:
        results.append(row["GroupAddressID"])

    return results


def get_group_addresses_for_line(conn, installation_id, area, line):
    cursor = conn.cursor(as_dict=True)
    cursor.execute("""SELECT DISTINCT GroupAddressID
  FROM
     [Connector]
      JOIN Device on (Connector.DeviceID = Device.ID)
      JOIN Line ON (Device.LineID = Line.ID)
      JOIN Area ON (Line.AreaID = Area.ID)
 WHERE
      Device.InstallationID = %s AND
      Area.Address = %d AND
      Line.Address = %d""", (installation_id, area, line))

    results = []
    for row in cursor:
        results.append(row["GroupAddressID"])

    return results


def get_all_devices_related_to_group_address(conn, installation_id, group_address_id):
    cursor = conn.cursor(as_dict=True)
    cursor.execute("""SELECT
    ConnectorSend = Connector.Send,
    ConnectorAcknowledge = Connector.Acknowledge,
    DeviceAddress = Device.Address,
    LineAddress = Line.Address,
    AreaAddress = Area.Address,
    IsActive = DeviceObject.IsActive,
    GroupAddressNo = GroupAddress.Address,
    ObjectSize = CASE WHEN DeviceObject.ObjectSize IS NOT NULL THEN DeviceObject.ObjectSize
      ELSE [dbo].[ufn_ReportDeviceObjectObjectSizeFromParents](CommunicationObjectRef.ID) END,
    DatapointType = CASE WHEN DeviceObject.DatapointType IS NOT NULL THEN DeviceObject.DatapointType
      ELSE [dbo].[ufn_ReportDeviceObjectDatapointTypeFromParents](CommunicationObjectRef.ID) END,
    ReadFlag = CASE when DeviceObject.ReadFlag IS NOT NULL Then DeviceObject.ReadFlag
      ELSE [dbo].[ufn_ReportDeviceObjectReadFlagFromParents](CommunicationObjectRef.ID) END,
    WriteFlag = CASE when DeviceObject.WriteFlag IS NOT NULL Then DeviceObject.WriteFlag
      ELSE [dbo].[ufn_ReportDeviceObjectWriteFlagFromParents](CommunicationObjectRef.ID) END,
    CommunicationFlag = CASE when DeviceObject.CommunicationFlag IS NOT NULL Then DeviceObject.CommunicationFlag
      ELSE [dbo].[ufn_ReportDeviceObjectCommunicationFlagFromParents](CommunicationObjectRef.ID) END,
    TransmitFlag = CASE when DeviceObject.TransmitFlag IS NOT NULL Then DeviceObject.TransmitFlag
      ELSE [dbo].[ufn_ReportDeviceObjectTransmitFlagFromParents](CommunicationObjectRef.ID) END,
    UpdateFlag = CASE when DeviceObject.UpdateFlag IS NOT NULL Then DeviceObject.UpdateFlag
      ELSE [dbo].[ufn_ReportDeviceObjectUpdateFlagFromParents](CommunicationObjectRef.ID) END,
    ReadOnInitFlag = CASE when DeviceObject.ReadOnInitFlag IS NOT NULL Then DeviceObject.ReadOnInitFlag
      ELSE [dbo].[ufn_ReportDeviceObjectReadOnInitFlagFromParents](CommunicationObjectRef.ID) END,
    Priority = CASE when DeviceObject.Priority IS NOT NULL Then DeviceObject.Priority
      ELSE [dbo].[ufn_ReportDeviceObjectPriorityFromParents](CommunicationObjectRef.ID) END,
    ReadableGroupAddress = [dbo].[ufn_ReportFormatGroupAddress](Connector.GroupAddressID, 0, ''),
    (SELECT C2.GroupAddressID FROM Connector C2 WHERE C2.DeviceObjectId = Connector.DeviceObjectID AND C2.Send = 1) as SendingGroupAddress,
    (SELECT [dbo].[ufn_ReportFormatGroupAddress](C2.GroupAddressID, 0, '') FROM Connector C2 WHERE C2.DeviceObjectId = Connector.DeviceObjectID AND C2.Send = 1) as ReadableSendingAddress
FROM
    [Connector]
      JOIN GroupAddress ON (Connector.GroupAddressID = GroupAddress.ID)
      JOIN Device on (Connector.DeviceID = Device.ID)
      JOIN Line ON (Device.LineID = Line.ID)
      JOIN Area ON (Line.AreaID = Area.ID)
      JOIN DeviceObject ON (Connector.DeviceObjectID = DeviceObject.ID)
      JOIN CommunicationObjectRef ON (CommunicationObjectRef.ID = DeviceObject.CommunicationObjectRefID)
WHERE
    DeviceObject.IsActive = 1
    AND Connector.GroupAddressID = %s
    AND Device.InstallationID = %s""", (group_address_id, installation_id))

    results = []
    for row in cursor:
        results.append(row)

    return results


def get_group_address_type_for_group_address(conn, group_address_id):
    cursor = conn.cursor(as_dict=True)
    cursor.callproc('[dbo].[usp_ReportGetGroupAddressType]', (group_address_id, ))
    for row in cursor:
        return row


def get_group_address_by_group_address_id(conn, group_address_id):
    cursor = conn.cursor(as_dict=True)
    cursor.execute("""SELECT Address
  FROM
     GroupAddress
 WHERE
      ID = %s""", group_address_id)

    for row in cursor:
        return row["Address"]


def merge_duplicate_devices(devices):
    merged_list = []
    for device in devices:
        in_list_item = find(lambda x: compare_device_for_merging(x, device), merged_list)
        if in_list_item is None:
            merged_list.append(device)
        else:
            index = merged_list.index(in_list_item)
            for a in ["ConnectorAcknowledge", "ReadFlag", "WriteFlag", "TransmitFlag", "UpdateFlag", "ReadOnInitFlag"]:
                merged_list[index][a] = merged_list[index][a] or device[a]
            merged_list[index]["Priority"] = merge_priority(merged_list[index]["Priority"], device["Priority"])

    return merged_list


def compare_device_for_merging(x, device):
    return device["AreaAddress"] == x["AreaAddress"] and \
        device["LineAddress"] == x["LineAddress"] and \
        device["DeviceAddress"] == x["DeviceAddress"] and \
        device["GroupAddressNo"] == x["GroupAddressNo"]


def merge_priority(priority1, priority2):
    priorities = ["Alert", "High", "Low"]
    return priorities[min(priorities.index(priority1), priorities.index(priority2))]


def filter_devices_inside_coupler(devices, area, line):
    if line == 0:
        return list(filter(lambda x: x["AreaAddress"] == area, devices))
    else:
        return list(filter(lambda x: x["AreaAddress"] == area and x["LineAddress"] == line, devices))


def filter_devices_outside_couplers(devices, area, line):
    if line == 0:
        return list(filter(lambda x: x["AreaAddress"] != area, devices))
    else:
        return list(filter(lambda x: x["AreaAddress"] != area or x["LineAddress"] != line, devices))


def filter_devices_read_flag(devices):
    return list(filter(lambda x: x["ReadFlag"], devices))


def filter_devices_write_flag(devices):
    return list(filter(lambda x: x["WriteFlag"], devices))


def filter_devices_transmit_flag(devices):
    return list(filter(lambda x: x["TransmitFlag"], devices))


def filter_devices_update_flag(devices):
    return list(filter(lambda x: x["UpdateFlag"], devices))


def filter_sending(devices):
    return list(filter(lambda x: x["ConnectorSend"], devices))


def find(f, seq):
    for item in seq:
        if f(item):
            return item
    return None


def get_label(option): return option.get('label')


def format_physical_address(device):
    return "%d.%d.%d" % (device["AreaAddress"], device["LineAddress"], device["DeviceAddress"])


if __name__ == "__main__":
    # execute only if run as a script
    cli()
