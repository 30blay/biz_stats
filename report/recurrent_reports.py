from report.report import Report
from etl.Metric import *
from etl.date_utils import last_month
from etl.DataWarehouse import Period
from etl.DataWarehouse import DataWarehouse
from sqlalchemy import create_engine


def bikeshare(date, gSheetId):
    """ Publish monthly bikeshare newsletter data """
    rep = Report(date, gSheetId)

    rep.add([
        BikeshareMau(),
        BikeshareUsers(),
        BikeshareTaps(),
        BikeshareUnlocksTotals(),
        BikeshareUnlocksUniques(),
    ], {
        'this month': Period(date, PeriodType.MONTH),
        'this month last year': Period(date.replace(year=date.year - 1), PeriodType.MONTH)
    })

    rep.add([
        BikeshareSales(),
    ], {
        'this month': Period(date, PeriodType.MONTH),
        'this month last year': Period(date.replace(year=date.year - 1), PeriodType.MONTH),
        'this year': Period(date, PeriodType.YEAR),
    })

    rep.add([
        BikeshareMostPopStations(),
    ], {
        '': Period(date, PeriodType.MONTH),
    })

    rep.df = rep.df.dropna(subset=['MAU this month'])
    rep.df = rep.df[rep.df.index != '(none)']

    rep.export_data_to_sheet('data')


def agencies(date, gSheetId):
    """ Publish monthly partner agencies newsletter data"""
    rep = Report(date, gSheetId)

    rep.set_code_groups({
        'CTTCT': ['CTTNH', 'CTTSTAM', 'CTTWAT', 'CTTHART', 'CTTNB', 'CTTMERI'],
        'Buenos Aires': ['SUBTAR', 'TRENAR', 'TRENSAR', 'COLEAR', 'COLECAR', 'COLECSAR', 'COLEPAR', 'COLEMAR', 'COLEC1AR', 'COLEC2AR', 'COLEC3AR', 'COLEC4AR', 'COLEC5AR', 'COLEC6AR', 'COLEC7AR', 'COLEC8AR'],
    })

    rep.add([
        AgencyLanguage(),
    ], {
        '': None
    })

    rep.add([
        AgencyAdoption(),
        AgencySessions(),
        AgencyDownloads(),
        AgencyMau(),
    ], {
        'this month': Period(date, PeriodType.MONTH),
        'this month last year': Period(date.replace(year=date.year - 1), PeriodType.MONTH)
    })

    rep.add([
        AgencyMostPopLines(),
    ], {
        '': Period(date, PeriodType.MONTH),
    })

    rep.add([
        AgencyGoTrips(),
    ], {
        'this month': Period(date, PeriodType.MONTH),
        'this month last year': Period(date.replace(year=date.year - 1), PeriodType.MONTH)
    })

    rep.add([
        AgencyAlertsSubs(),
        AgencySales(),
        AgencyTicketsSold(),
    ], {
        '': Period(date, PeriodType.MONTH),
    })

    rep.df = rep.df.fillna('')
    rep.df = rep.df.drop(index='(none)', errors='ignore')

    rep.export_data_to_sheet(sheet='data')


def stored_agencies(date, gSheetId):
    engine = create_engine('sqlite:///warehouse.db')

    warehouse = DataWarehouse(engine)
    rep = Report(date, gSheetId, warehouse=warehouse)

    periods = {
        'this month': Period(date, PeriodType.MONTH),
        'this month last year': Period(date.replace(year=date.year - 1), PeriodType.MONTH)
    }

    rep.add([
        AgencyAdoption(),
        AgencySessions(),
        AgencyDownloads(),
        AgencyMau(),
    ], periods)

    lines = warehouse.get_top_routes_and_hits(date, PeriodType.MONTH, this_period_last_year=True)
    rep.df = pd.merge(rep.df, lines, left_index=True, right_index=True, how='outer')

    rep.add([
        AgencyGoTrips(),
    ], periods)

    rep.add([
        AgencyAlertsSubs(),
        AgencySales(),
        AgencyTicketsSold(),
    ], {'this month': Period(date, PeriodType.MONTH)})

    rep.df = rep.df.fillna('')
    rep.df = rep.df[~rep.df.index.isna()]
    rep.df.index.rename('Feed Code', inplace=True)
    rep.export_data_to_sheet(sheet='data')


def kpi(date, gSheetId):
    """ Publish monthly KPIs"""
    rep = Report(date, gSheetId, groups_only=True)

    rep.set_code_groups({
        'Montreal': ['STM', 'RTL', 'STL', 'AMTTRAINS', 'AMTEXP', 'OMITSJU', 'MRCLM', 'MRC2M', 'MRCLASSO', 'CITVR', 'CITSO', 'CITSV', 'CITROUS', 'CITLR', 'CRTL', 'CITPI', 'CITLA', 'CITHSL', 'CITCRC', 'SJSRT', 'REMQC'],
        'New York': ['MTAS', 'MTAMNT', 'MTABK', 'MTABX', 'MTABC', 'MTAQN', 'MTASI', 'MTALIRR', 'MTAMNRR', 'NJTB', 'NJTR', 'PATH', 'SIF', 'NICE', 'BEEL', 'ATNY', 'NYW', 'RITNY', 'CWFNY', 'SEASNY'],
#        'Toronto': ['TTC', 'GOTRANSIT', 'YRT', 'GUELPH', 'HSR', 'GRT', 'BRPT', 'MIWAY', 'BTON', 'OTON', 'MILTON', 'DRTON', 'UPEON', 'TLTON', 'TTC1ON'],
#        'Paris': ['RATP', 'RATPS', 'RBUSFR', 'SQYBFR', 'STIFFR', 'STIF2FR', 'STIF0FR', 'TICEFR', 'TRAFR', 'STIVOFR'],
#        'Baltimore & DC': ['WMATA', 'DCC', 'MTGM', 'ART', 'FFC', 'VREVA', 'DASH', 'CUEDC', 'PRTCVA', 'DCSDC', 'PGCTBMD', 'BWTMD', 'CCCMD', 'MDTA', 'RTACMMD'],
    })
    rep.add([
        # AgencyRetention(),
        # AgencyDownloads(),
        # AgencyRatio(AgencyDailySessions(), AgencyDau()),
        # AgencyAdoption(),
        # AgencyMau(),
        # AgencyRatio(AgencyDau(), AgencyMau()),
        # AgencyRatio(AgencyTripPlans(), AgencyMau()),
        #
        # AgencyRatio(AgencySharedVehicleTaps(), AgencyMau()),
        # AgencyRidehailRequestsPlusTrips(),
        # AgencySharedVehicleUnlocks(),
        # AgencyRatio(AgencyGoTrips(), AgencyMau()),
        #
        # AgencyRatio(AgencyTransitAccounts(), AgencyMau()),
        AgencyTransitAccounts(),
        AgencyCreditCardAccounts(),
        AgencyDau(),
        # AgencySales(),
    ], {
        'this month': Period(date, PeriodType.MONTH),
        'last month': Period(last_month(date), PeriodType.MONTH),
        'this month last year': Period(date.replace(year=date.year - 1), PeriodType.MONTH)
    })


    rep.df = rep.df.fillna('')

    rep.export_data_to_sheet()


def latest_metrics(date, gSheetId):
    """ Publish to 'one sheet to rule them all', with recent data about every agency """

    engine = create_engine('sqlite:///warehouse.db')

    warehouse = DataWarehouse(engine)
    rep = Report(date, gSheetId)

    yearly = warehouse.slice_period(date, PeriodType.YEAR, [
        AgencyRidership(),
        AgencyRevenue(),
    ])

    monthly = warehouse.slice_period(date, PeriodType.MONTH, [
        AgencyDar(),
        AgencyDau(),
        AgencyMau(),
        AgencyAdoption(),
        AgencySales(),
        AgencyTicketsSold(),
        AgencyAlertsSubs(),
        SupportEmails(),
        AgencyRatio(AgencySales(), AgencyMau()),
        AgencyRatio(AgencyAlertsSubs(), AgencyMau()),
        AgencyRatio(AgencyDau(), AgencyMau())
    ])

    feeds = warehouse.get_feeds()
    feeds = feeds.set_index('feed_code')[['feed_name', 'feed_location', 'country_codes']]

    rep.df = pd.merge(feeds, yearly, left_index=True, right_index=True, how='outer')
    rep.df = pd.merge(rep.df, monthly, left_index=True, right_index=True, how='outer')

    rep.df = rep.df.rename(columns={
        'feed_name': 'Name',
        'feed_location': 'Municipality',
        'country_codes': 'Country code'
    })

    rep.df = rep.df.fillna('')
    rep.df.index.rename('Feed Code', inplace=True)
    rep.export_data_to_sheet(sheet='Latest', metrics_as_rows=False)


def rule_them_all(date, gSheetId):
    """ Publish to 'one sheet to rule them all', with recent data about every agency """
    rep = Report(date, gSheetId)

    rep.add([
        AgencyName(),
        AgencyLocation(name='Municipality'),
        AgencyCountry(),
        AgencyRidership(),
        AgencyRevenue(),
    ], {
        '': Period(last_month(date), PeriodType.YEAR),
    })

    rep.add([
        AgencyDar(),
        AgencyMau(),
        AgencyDau(),
        AgencyAdoption(),
        AgencySales(),
        AgencyTicketsSold(),
        AgencyAlertsSubs(),
        AgencyRatio(AgencySales(), AgencyMau()),
        AgencyRatio(AgencyAlertsSubs(), AgencyMau()),
        AgencyRatio(AgencyDau(), AgencyMau()),
    ], {
        '': Period(date, PeriodType.MONTH),
    })

    rep.df = rep.df.fillna('')

    rep.export_data_to_sheet(sheet='Latest', metrics_as_rows=False)