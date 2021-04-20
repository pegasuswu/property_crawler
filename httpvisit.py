from urllib.parse import urlencode
import time
import pymysql
import pymysql.cursors
from pymysql.converters import escape_string
import requests
from json import loads
import os
import pandas as pd
from city_geo_data import *
import configparser
import threading
import re

# http header to crawler realtor.ca from chrome.
headers_realtor = {
    'path': '/Listing.svc/PropertySearch_Post',
    'scheme': 'https',
    'accept': '*/*',
    'accept-encoding': 'gzip, deflate, br',
    'accepuaget-lang': 'zh-ChN,z;q=0.9',
    'content-type': 'application/x-www-form-urlencoded; charset=UTF-8',
    'cookie': 'visid_incap_2269415=uI80OmdxRvuSfUkZ8pHf6+MZa2AAAAAAQUIPAAAAAADBSCLEvKAbsshPPQzGVScf; _gid=GA1.2.1948861294.1617631717; gig_bootstrap_3_mrQiIl6ov44s2X3j6NGWVZ9SDDtplqV7WgdcyEpGYnYxl7ygDWPQHqQqtpSiUfko=gigya-pr_ver4; _fbp=fb.1.1617631717092.1445550965; visid_incap_2271082=RHnTlTuBTqmYnnhbphOnMxAaa2AAAAAAQUIPAAAAAABCBqI4BI41M44gvj3HCYiP; _gig_llp=googleplus; _gig_llu=%u8D85%u4EAE; ProfileTS=1617632576430; nlbi_2269415=LiQPepKbXm/r0acPqCntbAAAAADt7KqN18Hcfm+P6IvYo2ex; ASP.NET_SessionId=b5ibggkdsvrtnye3q5pagqja; nlbi_2271082=MyGqAYtd6WR5wMoyjZs+0QAAAABX2OdrFTTgoPPN3iElN04H; incap_ses_529_2269415=SKWAeVgzVW8Q17EaEmNXB/h9bGAAAAAA0fd8sbto68czrQfuphrdMw==; incap_ses_529_2271082=+6vOPY8cnkGaS7IaEmNXB01+bGAAAAAAF0B4D2tSOWlQ5C4e7U1YYA==; nlbi_2271082_2147483646=RjLZO9s9m0UbwNAljZs+0QAAAACzl2ySAuGQ55io9W87dJl+; reese84=3:/lTyt6U3vKbxDKUYChVsZg==:V7wR94rI0gg9BBMHmfYcvE+zYYmWWz3AVwF8QK47NS02SEH7muVX2HKK2/xhAm0dLtXSD5399SwUsr9dudD4ViBNmDGLemD265FDf5joeio/YKZRIcG64lcBilgJB5K/QWSLIH1X5/Yc5qbGQLXTs6bSj7gx0pcnf92oH8H9rRqMBG7ODH4yvZycKSTtZRiBDTegap2fBDKkOAaz88PBmwHnIb7Ls8Iil+kDHsKepyAsQHMD8CeFd87EZ+lr8U+jDy7RSG+mSOTpmymuddZrPCvBEfQSYlRrQF++Q01FXh8OACcknwshoGqhK4vUZVXnUf6H7TXRNpnDzBfFqoaCXt9A+6Dnio/YIyLWo4GwoBBOtMTCiU4+gUBQKqSvc8zcCKXU7Y1nISoPBR0X2HB0c7zfO3racdsx9NxtqmtDk6Y=:rRzmeW4X9YsOGiLDhtPT1jsdOyACk/naOSXCx/aQkII=; _dc_gtm_UA-12908513-11=1; _gat_UA-12908513-11=1; _gali=popularCityLinkCon; _4c_=jVLBjtsgFPyViHPsAMY2zm2VStVKrVpVW%2FUYYXiOURxjYRI3XeXf%2B3Cy8XV9QMwwM7z3zDuZWujJlhWsLLngORdSrskRriPZvhM9xPUSl7PvyJa0IQzjdrOZpin1oLrgfKrVhqyJdgZQwKq0Sini8A9RwkXcQ48RZPAG919f9r9fv0SlyAXjlMkijbcXGStZiYLBO3PWYR%2BuQwycoF6N5ogHBi5Ww36yJrTRzyVd2BbsoQ1Il3xmBx8laY77yfbGTYuPcrawT1%2FBou%2Bb6g9ndZg7idAdDmBWrzghEvwZkPrp3cX2Oip%2B9EF565DcuXMf%2FBW5neqVUUj9gtEa6INVnfM7dzqBt1rFGfrlBHW1d9MIsdpd690JVrJC1uH4yZ%2B5xBGhhwa8n1WIRhtgzvmY%2F4PDv7bQyUwPcfAsttI5vD3a8HejHHSwrl%2FkyL15i9367xBaZ%2FDkzStjo2qu00SxgUaduxBhnJnu1DhabWA8BjeQ25r8vT8lyWghqBQFvoOAPUtE8UOFt%2BbxpojKRVMKoRMBmUyEKYukBsUSXjVS6jzLZKbJI5NTzoWoKlHkj0yeV5x%2BpMbq7qFLiZ%2Fx2aftk4bL01HUpoamqZNcC5FgtzxRRquE1dTIoipprTPynEjJCypoKekjlMl74u32Hw%3D%3D; nlbi_2269415_2147483646=3LVeVzEOUxa7p0oNqCntbAAAAACTiU2+QFxHDXGywP7Yrw/y; _ga_Y07J3B53QP=GS1.1.1617722871.6.1.1617724253.46; _ga=GA1.2.1454120186.1617631717; reese84=3:W5TC/IWWmNniiwV1SAW6CQ==:YDiQfc7/VZlqeyX8iUecv4hIGF6ZAH/EsxqkDxg+Y1qJpQJ9nGRle0/qGDhbVwCXpMulaO/O9FI4DcOacq29GffwNgvsbdE2YJfWAvo7wFK6k+9mHB98g85OypxKXpcfKQZTR+EGyELfHBwud4P7lRT7IFMbrzA4qQkTzM0UB5iW77wl91dZXvw64xJqavOyjjMCDSjRb35ev+g9zpquOUN+ByUsWDhVOqJ3Y8RyVrnMex9Ldw3nRvKfFntH7kWesX8AYqFlLGQqQKUs+CrISdGRCBMkg9frFk/wGczSF5BT6xGE7ISu+/iNzSE8jeF5P3VyewCFrikjdnQ51B9730I2P68IpBI7tkUGA/4QiOLH9J6w2W0eSUP9299XazWgQ7bVpXJa7JNi13T8hh9xuCmRfZkRCUVsPFrO9azHA4Y=:F4wFlIcn3SKQyx5CbesHkNxqtc1sGGAxOZaVngXi3/U=',
    'origin': 'https://www.realtor.ca',
    'referer': 'https://www.realtor.ca/',
    'sec-ch-ua': '\"Google Chrome\";v=\"89\", \"Chromium\";v=\"89\", \";Not A Brand\";v=\"99\"',
    'sec-ch-ua-mobile': '?0',
    'sec-fetch-dest': 'empty',
    'sec-fetch-mode': 'cors',
    'sec-fetch-site': 'same-site',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.114 Safari/537.36',
    'Postman-Token': '01a0c953-439d-4ffe-8bda-012314efd58c',
    'Host': 'api2.realtor.ca',
    'Connection': 'keep-alive',
    # the length will be replaced by request.
    'Content-Length': 0
}


class Crawler(object):
    """
    newly_houseid_list : all the newly_houseid_list in database
    brokerid_list : all the brokerid_list in database
    init_houseid_list: the init houseid_list when program starts(
               not included the new records after program starts)
    init_brokerid_list: the init broker_list when program starts(
               not included the new records after program starts)
    res_dict: the dictionary of crawled records
    url_list: the list of newly-crawled records
    user_list: the user's list who use this program which is read from config file
    cityinfo_list: the city's list which contains several fileds which is read from config file
    ipaddr：the ipaddress of database instance
    port: the port of database instance
    username: the username of database
    """
    def __init__(self):
        self.newly_houseid_list = []
        self.brokerid_list = []
        self.init_houseid_list = []
        self.init_brokerid_list = []
        self.res_dict = {}
        self.url_list = []
        self.user_list = []
        self.cityinfo_list = []
        self.ipaddr = ''
        self.port = 3306
        self.username = ''
        self.password = ''
        self.url_realtor = 'https://api2.realtor.ca/Listing.svc/PropertySearch_Post'
        self.db_conn = 0

    # version 01 :read ini file
    # created time: 2021 / 04 / 19
    # in the version 02, will be modified to web page
    def read_config_ini(self):
        """
        Read the config file.
        Args：
           Object itself.
        Returns:
            none
        Created Time:2021/04/19
        """
        config = configparser.ConfigParser()
        current_work_dir_filename = os.getcwd() + '\\config\\config.ini'
        config.read(current_work_dir_filename, encoding='GB18030')

        user_number = config.getint('System', 'Usernum')
        for i in range(1, user_number + 1):
            user_section = 'user' + '%03d' % i
            temp_list = config.items(user_section)
            self.user_list.append(temp_list[0])
        city_number = config.getint('System', 'Citynum')

        for i in range(1, city_number + 1):
            city_section = 'city' + '%03d' % i
            temp_list = config.items(city_section)
            self.cityinfo_list.append(temp_list)

        # get some other basic info
        self.ipaddr = config.get('System', 'Ipaddress')
        self.port = config.getint('System', 'Port')
        self.username = config.get('System', 'Username')
        self.password = config.get('System', 'Password')

    def init_database(self):
        """ initial database, read the newly_houseid_list and brokerid_list
        Args:
            object itself.
        Returns:
            db_conn
        Created Time:2021/04/19
        """
        try:
            self.db_conn = pymysql.connect(host=self.ipaddr, port=self.port, user=self.username, password=self.password)
            self.db_conn.select_db('realtor')
            # get house_id info
            sql = 'select house_id from property_info'
            cur = self.db_conn.cursor()
            cur.execute(sql)
            restuple = cur.fetchall()
            if len(restuple) == 0:
                print('Hi,the property records is empty,run crawler to get data!')
            else:
                df = pd.DataFrame(list(restuple))
                # set two variables. if there are 100 thousand records.
                # The cost memory is just 100000*12,is about 1.2M,so
                # it does not need to optimization
                self.newly_houseid_list = []
                self.init_houseid_list = list(df[0])

            cur.close()
            # get broker_id info
            sql = 'select broker_id from broker_info'
            cur = self.db_conn.cursor()
            cur.execute(sql)
            restuple = cur.fetchall()
            if len(restuple) == 0:
                print('Hi,the broker records is empty,run crawler to get data!')
            else:
                df = pd.DataFrame(list(restuple))
                self.brokerid_list = list(df[0])
                self.init_brokerid_list = list(df[0])
        except pymysql.Error as err:
            print("Connect mysql error, please contact the admin")
            return False
        return True



    def parse_save_db(self, page_id, city_name):
        """
        parse the crawled page and save the important info to database
        :param page_id: start from 1 according to the realtor.ca
        :param city_name: the name of the crawling city
        :return: True for success or False for failur
        created time:2021/04/19
        """

        res_dict = self.res_dict
        if len(res_dict['Results']) == 0:
            print("Do not get any records in this crawling. Page:(%d) City:(%s)" % (page_id, city_name))
            return False
        records_perpage = len(res_dict['Results'])
        totalrecords = res_dict['Paging']['TotalRecords']
        if page_id == 1:
            print('In City(%s), We found %d records,now I will judge whether they have been crawled before.' %
                  (city_name, totalrecords))
        for i in range(records_perpage):
            list_property = res_dict['Results'][i]
            individualset = list_property['Individual'][0]

            # get the property info
            house_id = list_property['Id']
            if house_id in self.init_houseid_list:
                continue
            else:
                self.newly_houseid_list.append(house_id)

            mls_number = list_property['MlsNumber']
            publicremarks = list_property['PublicRemarks']

            judge_func = lambda a, b: b[a] if a in b else 'N'
            bathroomtotal = judge_func('BathroomTotal', list_property['Building'])
            bedrooms = judge_func('Bedrooms', list_property['Building'])
            housetype = judge_func('Type', list_property['Building'])
            ammenities = judge_func('Ammenities', list_property['Building'])

            individual_id = str(list_property['Individual'][0]['Organization']['OrganizationID'])
            propertyprice = list_property['Property']['Price']
            propertytype = list_property['Property']['Type']
            property_address = list_property['Property']['Address']['AddressText']
            if 'Photo' in list_property['Property']:
                propertyphoto_highres_path = list_property['Property']['Photo'][0]['HighResPath']
                propertyphoto_medres_path = list_property['Property']['Photo'][0]['MedResPath']
                propertyphoto_lowres_path = list_property['Property']['Photo'][0]['LowResPath']
                propertyphoto_last_updated = list_property['Property']['Photo'][0]['LastUpdated']
            else:
                propertyphoto_highres_path = "N"
                propertyphoto_medres_path = "N"
                propertyphoto_lowres_path = "N"
                propertyphoto_last_updated = "N"
            property_parking_spacetotal = judge_func('ParkingSpaceTotal', list_property['Property'])
            propertytype_id = list_property['Property']['TypeId']
            property_ownership_type = judge_func('OwnershipType', list_property['Property'])
            property_ammenities_nearby = judge_func('AmmenitiesNearBy', list_property['Property'])
            property_convertedprice = list_property['Property']['ConvertedPrice']
            property_parking_type = judge_func('ParkingType', list_property['Property'])
            property_price_unformatted_value = list_property['Property']['PriceUnformattedValue']
            property_video_link = judge_func('VideoLink', judge_func('AlternateURL', list_property))
            property_postalcode = list_property['PostalCode']
            property_relative_details_url = 'https://www.realtor.ca' + judge_func('RelativeDetailsURL', list_property)

            # new records to added for emailing to users
            self.url_list.append(property_relative_details_url)

            property_statusid = judge_func('StatusId', list_property)
            property_photo_change_date_utc = judge_func('PhotoChangeDateUTC', list_property)
            property_relative_urlen = 'https://www.realtor.ca' + list_property['RelativeURLEn']

            # then get the broker info
            broker_individual_id = judge_func('IndividualID', individualset)
            broker_name = judge_func('Name', individualset)
            broker_organizationid = individualset['Organization']['OrganizationID']
            broker_organization_name = individualset['Organization']['Name']
            broker_organization_phones = individualset['Organization']['Phones'][0]['AreaCode'] + '-' + \
                                         individualset['Organization']['Phones'][0]['PhoneNumber']

            # insert into database.because in the front, we have judged whether the records have been stored.
            # so theoretically there are no repetitive records
            sql_property = 'insert into property_info values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,' \
                           '%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
            insert_tup = (escape_string(house_id), escape_string(mls_number),
                          escape_string(publicremarks), escape_string(bathroomtotal),
                          escape_string(bedrooms), escape_string(housetype),
                          escape_string(ammenities), escape_string(individual_id),
                          escape_string(propertyprice), escape_string(propertytype),
                          escape_string(property_address), escape_string(propertyphoto_highres_path),
                          escape_string(propertyphoto_medres_path), escape_string(propertyphoto_lowres_path),
                          escape_string(propertyphoto_last_updated), escape_string(property_parking_spacetotal),
                          escape_string(propertytype_id), escape_string(property_ownership_type),
                          escape_string(property_ammenities_nearby), escape_string(property_convertedprice),
                          escape_string(property_parking_type), escape_string(property_price_unformatted_value),
                          escape_string(property_video_link), escape_string(property_postalcode),
                          escape_string(property_relative_details_url), escape_string(property_statusid),
                          escape_string(property_photo_change_date_utc), escape_string(property_relative_urlen))
            db_cur = self.db_conn.cursor()
            try:
                db_cur.execute(sql_property, insert_tup)
                self.db_conn.commit()
            except pymysql.ProgrammingError as progerr:
                print("ProgrammingError %s for run sql %s.City:(%s)" % (progerr, sql_property, city_name))
                progerrstr = str(progerr)
                # (1064, 'You have an error in your SQL syntax; ...
                mysqlerrorcode_match = re.search('^\((?P<mysqlErrorCode>\d+),', progerrstr)
                if mysqlerrorcode_match:
                    mysqlerror_code = mysqlerrorcode_match.group("mysqlErrorCode")
                    mysqlerrorcode_int = int(mysqlerror_code)
                    if mysqlerrorcode_int == 1064:
                        print("Debug: should pay attention for syntax error of mysql: %s" % sql_property)
                    elif mysqlerrorcode_int == 1062:
                        return True
                return False
            except Exception as err:
                # errType = type(err)
                # print("errType=%s" % errType) #<class 'pymysql.err.ProgrammingError'>
                # if repetitive records found ,do not report errors!
                if 'Duplicate entry' in err:
                    return True
                print("Error %s for execute sql: %s.City:(%s)" % (err, sql_property, city_name))
                self.db_conn.rollback()
                return False

            # insert the broker info to table broker
            # fields: broker_id(string),broker_name,broker_organization_id,broker_organization_name,\
            #                                                              broker_organization_phones
            if broker_individual_id in self.brokerid_list:
                continue
            else:
                self.brokerid_list.append(broker_individual_id)

            sql_broker = 'insert into broker_info values(%s,%s,%s,%s,%s)'
            insert_tup2 = (escape_string(str(broker_individual_id)), escape_string(broker_name),
                           escape_string(str(broker_organizationid)), escape_string(broker_organization_name),
                           escape_string(broker_organization_phones))

            db_cur = self.db_conn.cursor()
            try:
                db_cur.execute(sql_broker, insert_tup2)
                self.db_conn.commit()
            except pymysql.ProgrammingError as progerr:
                progerr_str = str(progerr)
                mysqlerrorcode_match = re.search("^\((?P<mysqlErrorCode>\d+),", progerr_str)
                if mysqlerrorcode_match:
                    mysqlerror_code = mysqlerrorcode_match.group("mysqlErrorCode")
                    mysqlerror_codeint = int(mysqlerror_code)
                    if mysqlerror_codeint == 1064:
                        print("Debug: should pay attention for syntax error of mysql: %s.City(%s)" % (
                              sql_broker, city_name))
                    elif mysqlerror_codeint == 1062:
                        return True
                return False
            except Exception as err:
                if err.args[1].find('Duplicate entry') >= 0:
                    return True
                print("Error %s for execute sql: %s.City(%s)" % (err, sql_broker, city_name))
                self.db_conn.rollback()
                return False
            db_cur.close()

        return True


def crawling_thread(crawler):
    """
    crawling thread function,Traversing the city_list according to the configuration to crawler realtor.ca
    :param crawler:
    :return: none
    created time:2021/04/20
    """
    while True:
        city_info_len = len(crawler.cityinfo_list)
        for i in range(city_info_len):
            city_list = crawler.cityinfo_list[i]
            crawler_func(crawler, city_list)
        # sleep for 2 minutes for avoiding page anti-crawling
        time.sleep(120)


def crawler_func(crawler, city_list):
    """
    crawling main function,according to designated city to crawl the realtor.ca
    :param crawler:
    :param city_list: city_list (included the name and some basic set rules)
    :return:True for success and False for failure
    """
    url_realtor = crawler.url_realtor
    city_name = city_list[0][1]
    if city_name == "markham":
        data_realtor = data_realtor_markham
    elif city_name == "toronto":
        data_realtor = data_realtor_toronto
    elif city_name == "vancouver":
        data_realtor = data_realtor_vancouver
    elif city_name == "calgary":
        data_realtor = data_realtor_calgary
    elif city_name == "ottawa":
        data_realtor = data_realtor_ottawa
    elif city_name == "edmonton":
        data_realtor = data_realtor_edmonton
    elif city_name == "mississauge":
        data_realtor = data_realtor_mississauge
    elif city_name == "montreal":
        data_realtor = data_realtor_montreal
    elif city_name == "hamilton":
        data_realtor = data_realtor_hamilton
    elif city_name == "surrey":
        data_realtor = data_realtor_surrey
    else:
        print("Do not support the city:(%s),exit" % city_name)
        return False

    # crawler the first page and get the basic info
    page_id = 1
    data_realtor['CurrentPage'] = page_id

    # update other fields.
    data_realtor['PriceMin'] = city_list[1][1]
    data_realtor['PriceMax'] = city_list[2][1]
    data_realtor['BedRange'] = city_list[3][1]
    data_realtor['BathRange'] = city_list[4][1]

    m_form_data2 = urlencode(data_realtor)
    headers_realtor['Content-Length'] = str(len(m_form_data2))
    res = ""
    try:
        res = requests.post(url=url_realtor, data=m_form_data2, headers=headers_realtor)
        res.raise_for_status() 
    except requests.HTTPError as e:
        print(e)
        print("status code", res.status_code)
        time.sleep(3)
        return False
    except requests.RequestException as e:
        print(e)
        return False
    crawler.res_dict = loads(res.content)
    return_value = crawler.parse_save_db(page_id, city_name)
    if return_value is False:
        return False
    # get the basic info by crawling first page
    totalpages = crawler.res_dict['Paging']['TotalPages']

    # crawle the rest pages.
    for page_id in range(2, totalpages + 1):
        if crawler.parse_save_db(page_id, city_name) is False:
            continue
    got_new_records_num = len(my_crawler.newly_houseid_list)
    print("We have got %d new records Now " % got_new_records_num)
    # if we have searched new records,than send them to user's email.
    retry_times = 0
    if got_new_records_num > 0:
        print("Now we will send Email to you!")
        # if failed to send email,try 3 times.if failed last ,then keep the url_list
        for retry_times in range(3):
            if send_mail_func(crawler) is True:
                crawler.init_houseid_list.extend(crawler.newly_houseid_list)
                crawler.newly_houseid_list = []
                crawler.url_list = []
                break

    # sleep 10 seconds after crawling every city.
    time.sleep(10)


def send_mail_func(crawler):
    """
    If we have found new records,send the email to user
    :param crawler:
    :return: True for success and False for failure.
    """
    ret = True
    my_sender = '1833717874@qq.com'  
    my_pass = 'iutxtdkbfhpzccae'  
    mail_text = ""

    if len(crawler.url_list) == 0:
        return True
    url_list_len = len(crawler.url_list)
    for i in range(url_list_len):
        mail_text = mail_text + crawler.url_list[i] + '<br>'

    #crawler.url_list = []
    try:
        user_list_len = len(crawler.user_list)
        for j in range(user_list_len):
            msg = MIMEText(mail_text, 'html', 'utf-8')
            msg['From'] = formataddr(("Realtor", my_sender),) 
            my_user = crawler.user_list[j][1]
            msg['To'] = formataddr(("Honey", my_user),)  
            msg['Subject'] = 'From crawler /realtor.ca Altogether {0} records'.format(url_list_len)
            server = smtplib.SMTP_SSL("smtp.qq.com", 465)
            server.login(my_sender, my_pass) 
            server.sendmail(my_sender, [my_user, ], msg.as_string())
            server.quit()
            print("Email have been sent to %s" % crawler.user_list[j][1])
    except Exception as err:
        print("Email sending failed.err=%s" % err)
        ret = False
    return ret


if __name__ == '__main__':
    """
    create crawling class init database and read config file
    then create crawling thread for crawling.
    """
    my_crawler = Crawler()
    my_crawler.read_config_ini()
    my_crawler.init_database()

    thread1 = threading.Thread(target=crawling_thread, args=(my_crawler,))
    thread1.start()
