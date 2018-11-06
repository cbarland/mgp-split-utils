##MGP Split Database Sync
##This program will create a .csv file PN BOM that is consistent with the MGP Split database

import pyodbc
import csv
import os
from collections import OrderedDict

ACCESS_DRIVER = (r'DSN=SQL server;Trusted_Connection=yes')
DROP_FILE = "//Jaguar/data/DATA/MGP Code Project/Reference/MGP PN - BOM C.csv"

def isSKU(sku):
    sku = sku.upper()
    types = ('S','F','R')
    if len(sku) == 11:
        if sku[:4].isdigit() and sku[5] in types:
            return True
        else:
            return False
    else:
        return False

def isSKULegacy(sku):
    return(isSKU(sku.replace('-','')))

class Database:
    TYPE_MAP = {'S':'Set', 'F':'Front', 'R':'Rear'}
    COLOR_MAP = {'RD':'Red', 'BK':'Black', 'MB':'Matte Black', 'YL':'Yellow'}
    def __init__(self):
        self.conn = pyodbc.connect(ACCESS_DRIVER)
        self.cursor = self.conn.cursor()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.cursor.close()
        self.conn.close()
        print('Connection closed')

    def get_bom_id(self, partNumber):
        SQL = "SELECT MGPPN, PartNumber_ID, Laser, Forming FROM tblMGPParts WHERE CHARINDEX(?,MGPPN) > 0;"
        values = partNumber
        self.cursor.execute(SQL,values)
        result = self.cursor.fetchone()
        try:
            return(result.PartNumber_ID, result.Laser, result.Forming)
        except AttributeError: return(0,0,0)
##        for row in self.cursor.execute(SQL, values):
##            return(row.MGPPN)

    def get_form(self, formID):
        if(formID==0):
            return('None')
        SQL = "SELECT DieID, Die FROM lstForming WHERE DieID = ?;"
        try:
            values = int(float(formID))
            self.cursor.execute(SQL,values)
            result = self.cursor.fetchone()
            return(result.Die)
        except (AttributeError, ValueError):
            return('None')

    def get_laser(self, laserID):
        if(laserID==0):
            return('None')
        SQL = "SELECT ProfileID, Profile FROM lstLaser WHERE ProfileID = ?;"
        try:
            values = int(float(laserID))
            self.cursor.execute(SQL,values)
            result = self.cursor.fetchone()
            return(result.Profile)
        except (AttributeError, ValueError):
            return('None')

    def fetch_bom(self, partNumber):
        bomID = (db_obj.get_bom_id(partNumber))
        profile = (db_obj.get_laser(str(bomID[1])))
        form = (db_obj.get_form(str(bomID[2])))
        return(profile, form)

    def get_part_numbers(self):
        SQL = "SELECT PartNumber FROM tblPartNumbers"
        self.cursor.execute(SQL)
        result = self.cursor.fetchall()
        return(result)

    def make_bom_str(self, partID):
        profile, form = self.fetch_bom(partID)
        return(partID+','+partID[:5]+',' +profile+','+form+'\n')

    def get_fitment(self, FGPN):
        SQL = """
SELECT tblAllSKU.SKU, tblFitment.VehicleYear AS [YEAR], tblFitment.Make AS MAKE, tblFitment.Model AS MODEL, tblFitment.Submodel AS [SUB MODEL], tblFitment.LinkNote AS [NOTE], [tblFitment].[MinWheelSize] AS [Min Wheel Size], tblExtendFitment.Region
FROM (tblFitment INNER JOIN (lstCategory INNER JOIN (tblAllSKU INNER JOIN tblAllSKU_Fitments_Join ON tblAllSKU.SKU_ID = tblAllSKU_Fitments_Join.SKU_ID) ON lstCategory.Category_ID = tblAllSKU.Category_ID) ON tblFitment.Fitment_ID = tblAllSKU_Fitments_Join.Fitment_ID) LEFT JOIN (tblExtendFitment RIGHT JOIN tblExtendFitment_Join ON tblExtendFitment.Extend_ID = tblExtendFitment_Join.Extend_ID) ON tblFitment.Fitment_ID = tblExtendFitment_Join.Fitment_ID
WHERE tblAllSKU.SKU=?
"""
        self.cursor.execute(SQL, FGPN)
        fitmentList = self.cursor.fetchall()
#         SQL = """
# SELECT ACESID, FinishedGoodPartNumber
# FROM tblFinishedGoodPartNumbers
# WHERE FinishedGoodPartNumber = ?
# """
#         values = FGPN
#         self.cursor.execute(SQL,values)
#         acesIDList = self.cursor.fetchall()
#
#         SQL = """
# SELECT PNID
# FROM tblPartNumbers
# WHERE PartNumber = ?
# """
#         values = int(FGPN[:5])
#         self.cursor.execute(SQL,values)
#         pnid = self.cursor.fetchone()[0]
#
#         fitmentList = []
#         for acesID in acesIDList:
#             SQL = """
# SELECT ACESID, YEAR, MAKE, MODEL, [SUB MODEL]
# FROM tblACESDescriptions
# WHERE ACESID = ?
# """
#             values = str(acesID.ACESID)
#             self.cursor.execute(SQL,values)
#             result = self.cursor.fetchone()
#
#             SQL = """
# SELECT ACESID, NOTE
# FROM tblACESTOMGPPartsJoin
# WHERE ACESID = ? AND PNID = ?
# """
#             values = (str(acesID.ACESID),pnid)
#             self.cursor.execute(SQL,values)
#             try: note = self.cursor.fetchone().NOTE
#             except AttributeError: note = ''
#             fitmentList.append((result,note,acesID.FinishedGoodPartNumber, acesID.ACESID))
        return(fitmentList)

    def get_pn_fitment(self, PN=None):
    #Returns an iterable of fitment data for the given 5-digit part number
        SQL = """
SELECT tblPartNumbers.PartNumber, tblFitment.VehicleYear AS [YEAR], tblFitment.Make AS MAKE, tblFitment.Model AS MODEL, tblFitment.Submodel AS [SUB MODEL], tblFitment.LinkNote AS [NOTE], [tblFitment].[MinWheelSize] AS [Min Wheel Size], tblExtendFitment.Region
FROM (tblFitment INNER JOIN (tblPartNumbers INNER JOIN tblAllSKU_Fitments_Join ON tblPartNumbers.PartNumber_ID = tblAllSKU_Fitments_Join.PartNumber_ID) ON tblFitment.Fitment_ID = tblAllSKU_Fitments_Join.Fitment_ID) LEFT JOIN (tblExtendFitment RIGHT JOIN tblExtendFitment_Join ON tblExtendFitment.Extend_ID = tblExtendFitment_Join.Extend_ID) ON tblFitment.Fitment_ID = tblExtendFitment_Join.Fitment_ID
"""
        #If no PN, return all PN fitments
        if PN is not None:
            SQL += "WHERE tblPartNumbers.PartNumber={0}".format(PN)

        self.cursor.execute(SQL)
        fitmentList = self.cursor.fetchall()
        return(fitmentList)

    def get_eng_description(self, engCode): ##Takes a string of 3-digit eng code or part number and returns a list of descriptions
        if isSKU(engCode):
            engCode = engCode[6:9]
        if len(engCode) != 3:
            raise ValueError("Value is not a valid SKU or engraving code: "+engCode)
        SQL = """
SELECT Description
FROM tblEngravingCodes
WHERE EngravingCode = ?
        """
        self.cursor.execute(SQL, engCode)
        result = self.cursor.fetchone()
        return(result.Description)

    def make_attributes_file(self):
        attibutes = {}
        partNumberList = self.get_part_numbers()
        for partNumber in partNumberList:
            attributes['SKU'] = partNumber
            attributes['type'] = TYPE_MAP[partNumber[7]]
            attributes['color'] = COLOR_MAP[partNumber[9:10]]
            attributes['graphic'] = get_eng_description(partNumber)

    def pnid_to_sku(self, pnid):
        SQL = """
FROM tblFinishedGoodPartNumbers
SELECT FinishedGoodPartNumber
WHERE ID=?
"""
        params = pnid
        cursor.execute(SQL,params)
        data = cursor.fetchone()
        return(data.FinishedGoodPartNumber)

    def get_paint_color(self, colorCode):
        if isSKU(colorCode):
            colorCode = colorCode[-2:]
        if len(colorCode) != 2:
            raise ValueError("Value is not a valid SKU or paint code: "+colorCode)
        SQL = """
SELECT Color, PaintFill
FROM "lstFinishColors"
WHERE ColorCode = ?
"""
        self.cursor.execute(SQL, colorCode)
        data = self.cursor.fetchone()
        return(data.Color, data.PaintFill)

    def get_set_type(self, basePN):
        if isSKU(basePN):
            basePN = basePN[:6]
        if len(basePN) != 6:
            raise ValueError("Value is not a valid SKU or Base PN: "+basePN)
        typeDict = {'S':('Set',4),
                    'F':('Front Set',2),
                    'R':('Rear Set',2)}

        specialTypeSets = {'57001S':3, '56001S':6}

        setType, quantity = typeDict[basePN[-1]]
        if basePN in specialTypeSets:
            quantity = specialTypeSets[basePN]

        return(setType, quantity)

    def get_sku_date(self, sku):
        SQL = """
SELECT ChangeDate
FROM tblAllSKU
WHERE SKU = ?
"""
        self.cursor.execute(SQL, sku)
        data = self.cursor.fetchone()
        if data is not None:
            return(data.ChangeDate)
        else:
            return('')

    def get_mfg_bom(self,basePN):
        SQL = "SELECT * FROM tblMGPParts WHERE InStr(MGPPN,?);"
        values = basePN
        self.cursor.execute(SQL,values)
        result = self.cursor.fetchall()
        partList = []
        for part in result:
            partDict = OrderedDict({"Plate Number":part.MGPPN,
                        "Note": part.Notes})

            partDict["Profile"] = self.list_query("lstLaser",
                                                  part.Laser,
                                                  "ProfileID",
                                                  "Profile")

            partDict["Forming"] = self.list_query("lstForming",
                                                  part.Forming,
                                                  "DieID",
                                                  "Die")

            partDict["Weld Fixture"] = self.list_query("lstWeldFixt",
                                                       part[14],
                                                       "WeldFixtID",
                                                       "WeldFixt")
            if part[3] is None:
                print(part[9])
                partDict["Bridge"] = self.list_query("lstBridgeTypeRH",
                                                     part[9],
                                                     "BridgeTypeID",
                                                     "BridgeType")
            else:
                partDict["Bridge RH"] = self.list_query("lstBridgeTypeRH",
                                                         part[9],
                                                         "BridgeTypeID",
                                                         "BridgeType")
                partDict["Bridge LH"] = self.list_query("lstBridgeTypeLH",
                                                         part[3],
                                                         "BridgeTypeLHID",
                                                         "BridgeTypeLH")
            if part[8] is not None:
                partDict["Alt Clip"] = self.list_query("lstAltClip",
                                                       part[8],
                                                       "AltClipID",
                                                       "AltClip")
            partDict["Lead In"] = part[16]

            partDict["Hole"] = self.list_query("lstHoleSize",
                                               part.Hole,
                                               "HoleSizeID",
                                               "HoleSize")
            partDict["Stud"] = self.list_query("lstStud",
                                               part.STUD,
                                               "StudID",
                                               "Stud")
            partDict["Nut"] = self.list_query("lstNut",
                                              part.NUT,
                                              "NutID",
                                              "Nut")

            partDict["Clip"] = self.list_query("lstClip",
                                               part.Clip,
                                               "ClipID",
                                               "Clip")
            if part.OriginalPN is not None:
                partDict["Original PN"] = part.OriginalPN
            partList.append(partDict)
        return(partList)

    def list_query(self, tableName, ident, idName, resultName):
        SQL = """
SELECT {2}, {3}
FROM {0}
WHERE {2} = {1}
""".format(tableName, ident, idName, resultName)
        try:
            self.cursor.execute(SQL)
        except:
            return None
        result = self.cursor.fetchone()
        if result is None:
            return None
        else:
            return(result[1])

    def get_make(self, sku):
        try:
            identifier = int(sku[:2])
        except:
            print("Error getting Make: {0}".format(sku[:2]))
            return()

        SQL = """
SELECT ManufacturerName, ManufacturerNumber
FROM tblManufacturerNumbers
WHERE ManufacturerNumber = ?
"""
        self.cursor.execute(SQL, identifier)
        try:
            return(self.cursor.fetchone()[0])
        except TypeError:
            return('Unknown')

    def set_primary_image(self, sku, imageName, imageURL):
        SQL = """
SET NOCOUNT ON;
DECLARE @DateTimeVal DATETIME;
SET @DateTimeVal = GETDATE();

UPDATE tblAllSKU
SET PrimaryImageName = ?, PrimaryImageURL = ?, ChangeDate = @DateTimeVal
WHERE SKU = ?
"""
        self.cursor.execute(SQL, imageName, imageURL, sku)
        self.conn.commit()

    def get_primary_image(self, sku):
        SQL = """
SELECT PrimaryImageName, PrimaryImageURL
FROM tblAllSKU
WHERE SKU = ?
"""
        self.cursor.execute(SQL, sku)
        data = self.cursor.fetchone()
        return(data[0], data[1])

    def get_sku_images(self, sku = None):
        ##Iterate through dataframe and get each rear image
        rSQL = """
SELECT lstImage.ImageName AS [RearImage], lstImage.ImageURL As [RearURL]
FROM tblAllSKU INNER JOIN lstImage ON tblAllSKU.Rear_Image_ID = lstImage.Image_ID
WHERE tblAllSKU.SKU = ?
"""
        dataList = []

        #If sku is not provided, fetch all SKUs. Othwise fetch that sku
        if sku is None:
            SQL = """
SELECT tblAllSKU.SKU, lstImage.ImageName AS [FrontImage], lstImage.ImageURL As [FrontURL], tblAllSKU.SetOf
FROM tblAllSKU INNER JOIN lstImage ON tblAllSKU.Front_Image_ID = lstImage.Image_ID
WHERE tblAllSKU.Category_ID=?"""

            self.cursor.execute(SQL, 1)
            data = self.cursor.fetchall()

        else:
            SQL = """
SELECT tblAllSKU.SKU, lstImage.ImageName AS [FrontImage], lstImage.ImageURL As [FrontURL], tblAllSKU.SetOf
FROM tblAllSKU INNER JOIN lstImage ON tblAllSKU.Front_Image_ID = lstImage.Image_ID
WHERE tblAllSKU.Category_ID = ? AND tblAllSKU.SKU = ?"""

            self.cursor.execute(SQL, 1, sku)
            data = self.cursor.fetchone()

        for row in data:
            try:
                self.cursor.execute(rSQL, row.SKU)
                rearData = self.cursor.fetchone()

                dataList.append({"SKU":row.SKU,
                                "Front Image": row.FrontImage,
                                "Rear Image": rearData.RearImage,
                                "Set Of": row.SetOf,
                                "Front URL": row.FrontURL,
                                "Rear URL": rearData.RearURL})
            except AttributeError:
                try:
                    self.cursor.execute(rSQL, data.SKU)
                    rearData = self.cursor.fetchone()

                    dataList.append({"SKU":data.SKU,
                                    "Front Image": data.FrontImage,
                                    "Rear Image": rearData.RearImage,
                                    "Set Of": data.SetOf,
                                    "Front URL": data.FrontURL,
                                    "Rear URL": rearData.RearURL})
                except AttributeError as e:
                    print("Image not found for SKU {0}".format(data))
                    print(e)
                break
        return(dataList)

if __name__ == '__main__':
    with Database() as db_obj:
        print('Getting part numbers from ODBC database...')
        partNumberList = db_obj.get_part_numbers()

        bomlist = 'MGPPN,PartNumber,Profile,Die\n'

        print('Cross-referencing part number IDs...')
        for partNumber in partNumberList:
            print(partNumber)
            frontPartID = str(partNumber)[1:6]+'-F'
            rearPartID = str(partNumber)[1:6]+'-R'
            bomlist = bomlist+(db_obj.make_bom_str(frontPartID)+ db_obj.make_bom_str(rearPartID))
        with open(DROP_FILE , 'w') as bom_file:
            bom_file.write(bomlist)

        print('BOM update successful.')
