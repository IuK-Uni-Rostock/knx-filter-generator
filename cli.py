import click
import pymssql
from pick import pick

from gen import generate_rules_for_coupler


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
    count = []
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


def get_group_address_by_group_address_id(conn, group_address_id):
    cursor = conn.cursor(as_dict=True)
    cursor.execute("""SELECT Address
  FROM
     GroupAddress
 WHERE
      ID = %s""", group_address_id)

    for row in cursor:
        return row["Address"]


def get_label(option): return option.get('label')


if __name__ == "__main__":
    # execute only if run as a script
    cli()
