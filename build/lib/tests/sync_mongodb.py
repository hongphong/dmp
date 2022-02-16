# from dmp.helper.utils import generate_batches
# from dmp.df.pandas import PandasPipeline
# a = PandasPipeline.connect_db("hssk_mongo2_view")
# cursor = a.read_mongodb("Account","colombo10263", query={},  return_cursor=True)
# col = ['_id', 'createdTime', 'createdYear', 'createdMonth', 'createdDay',
#        'lastUpdateTime', 'lastUpdateDate', 'sortOrder', 'creatorId', 'site',
#        'createdEmployeeId', 'createdOrganizationId', 'type', 'fullName',
#        'name', 'code', 'email', 'address', 'phone', 'userCode', 'username',
#        'userId', 'createdMongoDate', 'sites', 'type1', 'type2',
#        'lastUpdateUserId', 'resellerId', 'privilegeCache', 'status', 'grades',
#        'sortFullName', 'type3', 'title', 'password', 'jobTitle', 'province',
#        'confirmPassword', 'passPort', 'gender', 'district', 'image', 'groups',
#        'groupSettings', 'privileges', 'allPrivileges', 'birthDate', 'subjects',
#        'components', 'lastActiveTime', 'lastSessionId', 'lastClientIP',
#        'provinceId', 'districtId', 'totalNewNotification', 'note',
#        'provinceTitle', 'dob', 'website',
#        'brief', 'idCard', 'lastLoginTime', 'lastReadNotification',
#        'districtTitle', 'accounts', 'accountClosure', 'comlinkId',
#        'comlinkPassword', 'comlinkUsername', 'departmentIds',
#        'departmentTitle', 'fax', 'cover', 'avatar', 'groupId', 'closures',
#        'requireChangePassword', 'suggestTitle', 'hideDashboardModules']
# n = 0
#
#
# def process_item(x):
#     data = {k: x.get(k, None) for k in col}
#     data['_id'] = str(data['_id'])
#     return data
#
# from dmp.helper.connection import WfConnection
# con = WfConnection("dim_accounts")
# engine = con.get_wf_db().get_sqlalchemy_engine()
#
# for batch in generate_batches(cursor, batch_size_limit=10000, callback_item=lambda x: {k: x.get(k, None) for k in col}):
#     df = PandasPipeline.read_dict(batch).normalize_cols(inplace=False).astype(str).cast_cols({
#         'createdtime': ("int", {"errors": "coerce"}),
#         'createdyear': ("int", {"errors": "coerce"}),
#         'createdmonth': ("int", {"errors": "coerce"}),
#         'createdday': ("int", {"errors": "coerce"}),
#         'lastupdatetime': ("int", {"errors": "coerce"}),
#         'sortorder': ("int", {"errors": "coerce"}),
#         'creatorid': ("int", {"errors": "coerce"}),
#         'site': ("int", {"errors": "coerce"}),
#         'createdemployeeid': ("int", {"errors": "coerce"}),
#         'createdorganizationid': ("int", {"errors": "coerce"}),
#         'userid': ("int", {"errors": "coerce"}),
#         'resellerid': ("int", {"errors": "coerce"}),
#         'status': ("int", {"errors": "coerce"}),
#         'lastactivetime': ("int", {"errors": "coerce"}),
#         'lastsessionid': ("int", {"errors": "coerce"}),
#         'provinceid': ("int", {"errors": "coerce"}),
#         'districtid': ("int", {"errors": "coerce"}),
#         'groupid': ("int", {"errors": "coerce"}),
#         "comlinkid": ("int", {"errors": "coerce"}),
#         "departmentids": ("int", {"errors": "coerce"})})
#     n += len(batch)
#     PandasPipeline.read_df(df).save_to_db_sql("dim_accounts", to_wf_connection_id="dw_oracle", mode="replace",
#                                               index=False)
#     print("processed: %s. Last _id: %s" % (n, batch[-1]['_id']))
#
#
intcol = {
    'createdtime': ("int", {"errors": "coerce"}),
    'createdyear': ("int", {"errors": "coerce"}),
    'createdmonth': ("int", {"errors": "coerce"}),
    'createdday': ("int", {"errors": "coerce"}),
    # 'lastupdatetime': ("int", {"errors": "coerce"}),
    # 'sortorder': ("int", {"errors": "coerce"}),
    # 'creatorid': ("int", {"errors": "coerce"}),
    # 'site': ("int", {"errors": "coerce"}),
    # 'createdemployeeid': ("int", {"errors": "coerce"}),
    # 'createdorganizationid': ("int", {"errors": "coerce"}),
    # 'userid': ("int", {"errors": "coerce"}),
    # 'resellerid': ("int", {"errors": "coerce"}),
    # 'status': ("int", {"errors": "coerce"}),
    # 'lastactivetime': ("int", {"errors": "coerce"}),
    # 'lastsessionid': ("int", {"errors": "coerce"}),
    # 'provinceid': ("int", {"errors": "coerce"}),
    # 'districtid': ("int", {"errors": "coerce"}),
    # 'groupid': ("int", {"errors": "coerce"}),
    # "comlinkid": ("int", {"errors": "coerce"}),
    # "departmentids": ("int", {"errors": "coerce"})
    }
allcol = """
#         id CLOB,
#         createdtime CLOB,
#         createdyear CLOB,
#         createdmonth CLOB,
#         createdday CLOB,
#         lastupdatetime CLOB,
#         lastupdatedate CLOB,
#         sortorder CLOB,
#         creatorid CLOB,
#         site CLOB,
#         createdemployeeid CLOB,
#         createdorganizationid CLOB,
#         type CLOB,
#         code CLOB,
#         title CLOB,
#         parentid CLOB,
#         content CLOB,
#         latitude CLOB,
#         longitude CLOB,
#         countryid CLOB,
#         normalizetitle CLOB,
#         normalizeparenttitle CLOB,
#         tempidx CLOB,
#         type1 CLOB,
#         type2 CLOB,
#         studycode CLOB,
#         studyid CLOB,
#         oldcode CLOB,
#         englishname CLOB,
#         leveltitle CLOB,
#         viettelsaleid CLOB,
#         zipcode CLOB,
#         bccscode CLOB,
#         cdcid CLOB,
#         cdccode CLOB,
#         englishtitle CLOB,
#         status CLOB,
#         nationid CLOB
"""

allcol = [k.replace("#", "").replace(" CLOB", "").replace(",","").strip() for k in allcol.split("\n")]
for k in allcol:
    if k.strip() == "":
        continue
    print(("CAST(TRIM(CAST({k} as VARCHAR(400))) as NUMBER) as {k}".replace("{k}", k) if intcol.get(k) else
           "DBMS_LOB.substr({k},0,4000) as {k}".replace("{k}",k)
           )+",")
# print(allcol)
