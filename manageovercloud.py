"""
MIT License 
Copyright 2023 Reeyarn Li
VERSION 2023 May; 1.0.0520

# DOCUMENTATION

This class replaces local file IO functions such as os.path.isfile(), os.path.isdir(), open(...).read(), open(...).write()  and operates flexibles on local storage and dropbox cloud.

When provided with a dropbox access token, this class module can use Dropbox Python SDK API to do the above operations on Dropbox cloud.
When use_localfs, use_dropbox = True, Flase, it reduces to os. method
When use_localfs, use_dropbox = True, True,  it will write to both, but read from local
When sync_if_missing_file = True, when one file exists in one location but not the other, it will attempt to sync to the other location.


# MIT License        

The MIT License is a permissive open-source license that allows users to freely use, modify, distribute, and sublicense the code, both for commercial and non-commercial purposes, without any additional restrictions. The MIT License is one of the most popular open-source licenses due to its simplicity and permissiveness. It allows developers to collaborate and build upon existing codebases, fostering a culture of sharing and innovation within the software development community. By including the MIT License in the code, the author grants others the right to use, copy, modify, merge, publish, distribute, sublicense, and/or sell the code and its accompanying documentation, while also disclaiming any warranties or liabilities associated with the code. The license ensures that users can freely use the code without fear of legal repercussions, as long as they include the original copyright notice and disclaimers.

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions: 
* The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.
                
THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
"""


# Import Python's native modules
import datetime
import os
import gzip 
from typing import Union

# Import PIP packages
import logging
import dropbox


logger = logging.getLogger(__name__)

class ManageOvercloud(object):
    """
    This class replaces local file IO functions such as os.path.isfile(), os.path.isdir(), open(...).read(), open(...).write() 
        and operates flexibles on local storage and dropbox cloud.
    When provided with a dropbox access token, this class module can use Dropbox Python SDK API to do the above operations on Dropbox cloud.
    When use_localfs, use_dropbox = True, Flase, it reduces to os. method
    When use_localfs, use_dropbox = True, True,  it will write to both, but read from local
    When sync_if_missing_file = True, when one file exists in one location but not the other, it will attempt to sync to the other location.

    Dropbox python API documentation:
    https://dropbox-sdk-python.readthedocs.io/en/latest/index.html
    
    self= LocalCloudHandler(True, True)
    rel_path = "/tmp2/d2"
    """
    #self.dbx=None

    def __init__(self, use_localfs = True, use_dropbox = False, 
                 local_prefix = "", cloud_prefix = "", 
                 dropbox_app_key:str, dropbox_app_secret: str,
                 sync_if_missing_file=False):
        logger.info(f"LocalCloud init... \n  Use Local HD: {use_localfs}, local_prefix = {local_prefix} ;\n  Dbx Cloud {use_dropbox}, mylc cloud_prefix = {cloud_prefix}.")    
        self.use_localfs  = use_localfs
        self.use_dropbox  = use_dropbox
        self.local_prefix = local_prefix
        self.cloud_prefix = cloud_prefix
        self.sync_if_missing_file = sync_if_missing_file
        self.dropbox_app_key = dropbox_app_key
        self.dropbox_app_secret = dropbox_app_secret

        if self.use_dropbox:
            access_token = self.get_existing_dropbox_token()
            dbx = self.connect_dropbox(access_token)
            if dbx == None:
                self.use_dropbox = False; 
            else:
                self.dbx = dbx
                
        logger.info(f"Finished init. Dbx Cloud status {self.use_dropbox}")

    @staticmethod
    def _remove_doubleslash_endslash(rel_path):
        while '//' in rel_path:
            rel_path = rel_path.replace('//', '/')
        if rel_path.endswith("/"):
            rel_path = rel_path[:len(rel_path)-1]            
        return rel_path        
    
    def makedirs(self, rel_path):
        """Tested on both: DONE
        rel_path = '/text/edgar/by-index/abc'
        rel_path = '/text/edgar/'
        """
        rel_path = self._remove_doubleslash_endslash(rel_path)
        if self.use_localfs:
            local_full_path=self.local_prefix + rel_path
            if not os.path.exists(local_full_path):
                os.makedirs(local_full_path, exist_ok=True)
                logger.info(f"Make dir in local filesystem: {local_full_path}")
        if self.use_dropbox:
            cloud_full_path = self.cloud_prefix + rel_path    
            cloud_full_path = self._remove_doubleslash_endslash(cloud_full_path)
            try: 
                self.dbx.files_create_folder_v2(cloud_full_path)
                logger.info(f"Make dir in dbx-cloud filesystem: {cloud_full_path}")
            except dropbox.exceptions.ApiError as e:
                logger.error(f"Failed to create directory {cloud_full_path}, {str(e)}")
                    
    def rename(self, source, destination):
        """
        source = "/tmp/d1"
        destination = "/tmp/d3"
        Tested with dropbox
        """
        if self.use_localfs:
            if not os.path.exists(self.local_prefix + destination):
                # Move the file
                if os.path.exists(self.local_prefix + source):
                    os.rename(self.local_prefix + source, self.local_prefix + destination)
                    logger.debug("Local Move/Rename successful: {} -> {}".format(self.local_prefix + source, self.local_prefix + destination))
                else: 
                    logger.warning("Source {} does not exist. Move aborted.".format(self.local_prefix + source))    
            else:
                logger.warning("Destination {} already exists. Move aborted.".format(self.local_prefix + destination))    
        if self.use_dropbox:
            try:
                full_dest = self._remove_doubleslash_endslash(self.cloud_prefix + destination)                    
                full_from = self._remove_doubleslash_endslash(self.cloud_prefix + source)
                self.dbx.files_move(full_from, full_dest)
                logger.debug("Dbx Cloud Move/Rename successful: {} -> {}".format(full_from, full_dest))
            except dropbox.exceptions.ApiError as e:
                if isinstance(e.error, dropbox.files.RelocationError):
                    logger.warning("A conflict occurred. The destination already exists, or the source does not exist")
                else:
                    logger.critical("While trying to rename in Dropbox cloud, Undefined Exception occurred " + str(e))       

    def remove(self, rel_path):
        return_value, local_return_value, dbx_return_value = None, None, None
        local_full_path = self.local_prefix + rel_path
        cloud_full_path = self._remove_doubleslash_endslash(self.cloud_prefix + rel_path)
        if self.use_localfs:
            try:
                os.remove(local_full_path)
                logger.info(f"Deleted file from local filesystem {local_full_path}")
                local_return_value = True
            except OSError as e:
                logger.info(f"Unable to delete file from local filesystem {local_full_path} " + str(e))
        else:
            local_return_value = True        
        if self.use_dropbox:
            try: 
                dbx.files_delete_v2(cloud_full_path)
                dbx_return_value = True
            except dropbox.exceptions.ApiError as e:
                logger.info(f"Unable to delete file from dropbox filesystem {cloud_full_path} " + str(e))
        else:
            dbx_return_value = True        
        return_value = local_return_value and dbx_return_value        
        return return_value
                               
    def listdir(self, rel_path):
        """
        Syncing if missing file is not implemented for ManageOvercloud.listdir(); but the returned list is the subset of files that exist on both locations
        """
        return_value, local_return_value, dbx_return_value = [], [], []
        local_full_path = self.local_prefix + rel_path
        cloud_full_path = self._remove_doubleslash_endslash(self.cloud_prefix + rel_path)
        if self.use_localfs:
            local_return_value = os.listdir(local_full_path)
        if self.sync_if_missing_file or not local_return_value:  
            if self.use_dropbox:  
                try:
                    result = dbx.files_list_folder(cloud_full_path)
                    dbx_return_value = [entry.name for entry in result.entries]                
                except Exception as e:
                    logging.critical("Error listing dropbox folder {cloud_full_path}")    
        if self.sync_if_missing_file:
            logger.debug("Syncing if missing file is not implemented for ManageOvercloud.listdir(); but the returned list is the subset of files that exist on both locations")
            return_value = [folder for folder in local_return_value if folder in dbx_return_value]
        else:    
            return_value = local_return_value or dbx_return_value
        return return_value
                        
    def path_exists(self, rel_path ):
        """
        DEPRECIATED. 
        
        USE path_isfile or path_isdir instead
        rel_path = os.path.expanduser("~/Dropbox/Codes/")
        rel_path = '/text/edgar/full-index/1998-QTR4.csv.gz'
        rel_path='/text/edgar/by-index/'
        """
        logger.critical("Should not have called LocalCloud.path_exists(). Call LocalCloud.path_isfile() or LocalCloud.path_isdir() instead")
        return_value = False
        if self.use_localfs:
            local_full_path=self.local_prefix + rel_path
            local_return_value = os.path.exists(local_full_path)
            return_value =  return_value or local_return_value
            logger.debug(f"Local path_exists {local_full_path} local result: {local_return_value}")
        if not return_value:        
            if self.use_dropbox:
                cloud_full_path = self._remove_doubleslash_endslash(self.cloud_prefix + rel_path)
                try:
                    dbx_return_value = self.dbx.files_get_metadata(cloud_full_path)
                    logger.debug(f"Dbx Cloud path {cloud_full_path} exists.")
                    return_value = return_value or bool(dbx_return_value)
                except dropbox.exceptions.ApiError as e:
                    return_value =  return_value or  False
        return return_value
      
    def path_isfile(self, rel_path, check_onlyone_overrule = False):
        """
        rel_path = '/text/edgar/by-index/year2021_10k.csv'
        rel_path = "/text/edgar"
        self = mylc
        In general, when self.sync_if_missing_file = False, this function returns True as long as file exists in one place.
        When self.sync_if_missing_file =True, globally relative to the whole LocalCloud() instance, this function returns True only when files exist in both places; otherwise it will upload/download to make it happen when the file exists in only one place, and return False
        When check_onlyone_overrule = True, as long as file exists in one place before invoking this function, this function returns true, regardless of self.sync_if_missing_file and the upload/download action it may trigger within this function.
        Use check_onlyone_overrule = True for checking raw files directly from SEC to avoid redundant download when the file already exists in one place.
        """
        return_value, local_return_value, dbx_return_value = False, False, False
        local_full_path=self.local_prefix + rel_path
        cloud_full_path = self._remove_doubleslash_endslash(self.cloud_prefix + rel_path)
        if self.use_localfs:
            local_return_value =  os.path.isfile(self.local_prefix + rel_path)
            logger.debug(f"Local path_isfile {local_full_path} result: {local_return_value}")
        if self.sync_if_missing_file or not local_return_value:
            if self.use_dropbox:
                try:
                    metadata = self.dbx.files_get_metadata(cloud_full_path, include_media_info=True)
                    if  isinstance(metadata, dropbox.files.FileMetadata):
                        dbx_return_value = True
                        logger.debug(f"Dbx Cloud path_isfile exists: {cloud_full_path}")
                        if self.sync_if_missing_file and not local_return_value:
                            # Need to download from Dropbox to local
                            logger.debug(f"""Local file {local_full_path} not existing, but found in dropbox {cloud_full_path}. With --sync-if-missing-file, Start Downloading.""")
                            self.dbx_download(cloud_full_path, local_full_path)
                            logger.info(f"""Local file {local_full_path} not existing, but found in dropbox {cloud_full_path}. With --sync-if-missing-file, Finished Downloading.""")
                    elif isinstance(metadata, dropbox.files.FolderMetadata):
                        logger.info(f"Dbx Cloud path_isfile checking: {cloud_full_path} may exist but is a folder")    
                    else:
                        logger.info(f"Dbx Cloud path_isfile checking: {cloud_full_path} may exist but not a file")    
                except:
                    logger.debug(f"Dbx Cloud path_isfile checking FAILED: {cloud_full_path} ")    
                if self.sync_if_missing_file and self.use_localfs and not dbx_return_value:
                    # Need to Upload from local to dropbox.
                    logger.debug(f"""Local file {local_full_path}  existing, but not found in dropbox {cloud_full_path}. With --sync-if-missing-file, Start Reading local file.""")
                    cloud_parent_folder = os.path.join(*rel_path.split("/")[:-1])
                    if not self.path_isdir(cloud_parent_folder, check_both=True):
                        self.makedirs(cloud_parent_folder)
                    with open(local_full_path, "rb") as f:
                        bytes_data = f.read()
                        logger.debug(f"""Local file {local_full_path}  existing, but not found in dropbox {cloud_full_path}. With --sync-if-missing-file, Start Uploading local file.""")
                        self.dbx_upload(bytes_data, cloud_full_path)
                        logger.info(f"""Local file {local_full_path}  existing, but not found in dropbox {cloud_full_path}. With --sync-if-missing-file, Finished Uploading local file.""")
        if self.sync_if_missing_file:    
            return_value = local_return_value and dbx_return_value
        else:    
            return_value = local_return_value or dbx_return_value
        if check_onlyone_overrule:
            return_value = local_return_value or dbx_return_value
        return return_value
    
    def path_isdir(self, rel_path, check_both=False):
        return_value, local_return_value, dbx_return_value = False, False, False
        if self.use_localfs:
            local_full_path= self.local_prefix + rel_path
            local_return_value = os.path.isdir(local_full_path)
            logger.debug(f"Local path_isdir {local_full_path} result: {local_return_value}")
        if check_both or self.sync_if_missing_file or not local_return_value:      
            if self.use_dropbox :
                cloud_full_path=self._remove_doubleslash_endslash(self.cloud_prefix + rel_path)
                try:
                    #dbx_return_value = dbx.files_list_folder(cloud_full_path)
                    dbx_return_value = False
                    metadata = self.dbx.files_get_metadata(cloud_full_path, include_media_info=True)
                    if isinstance(metadata, dropbox.files.FolderMetadata):
                        logger.debug(f"Dbx Cloud path_isdir dbx-cloud result: {cloud_full_path}")
                        dbx_return_value = True
                except:
                    logger.debug(f"Dbx Cloud path_dir checking FAILED: {cloud_full_path} ")    
        if check_both or self.sync_if_missing_file:
            return_value = local_return_value and bool(dbx_return_value)
        else:    
            return_value = local_return_value or bool(dbx_return_value)
        return return_value
    
    def dbx_upload(self, f: bytes, dbx_full_path):
        while '//' in dbx_full_path:
            dbx_full_path = dbx_full_path.replace('//', '/')
        dbx_full_path = self._remove_doubleslash_endslash(dbx_full_path)
        use_dropbox = False
        if self.use_dropbox:
            use_dropbox = True
        elif "dbx" in globals():
            use_dropbox = True
        if use_dropbox:
            if len(f)< 150_000_000:
                self.dbx.files_upload(f, dbx_full_path, dropbox.files.WriteMode.overwrite)
            else: 
                logger.critical(f"Cannot upload to dropbox with size bigger than 150M: {dbx_full_path}") 
        else:
            logger.critical(f"use_dropbox = False but called dbx_upload() for file {dbx_full_path}")    

    def dbx_download(self, dbx_full_path, local_full_path = None):
        """
        read a file from dropbox cloud, and return it as bytes value, 
        if provided local_full_path, then saved as local file
        """
        dbx_full_path = self._remove_doubleslash_endslash(dbx_full_path)    
        use_dropbox = False
        if self.use_dropbox:
            use_dropbox = True
        elif "dbx" in globals():
            use_dropbox = True
        if use_dropbox:
            try:
                md, res = self.dbx.files_download(dbx_full_path)
                data = res.content
                logger.info(f"Read from dropbox cloud file {dbx_full_path}")
                if local_full_path:
                    with open(local_full_path, "wb") as file:
                        file.write(data)
                        logger.info(f"Save to local file {local_full_path}")
            except dropbox.exceptions.HttpError as err:
                logger.critical('***  dbx_download HTTP error', err)
        else:
            logger.critical("use_dropbox = False but called dbx_download()")    
        return data

    def write(self, data: Union[bytes, str], rel_path, use_gzip=False):
        """
        write(...) allows writing to both local storage and dropbox cloud;
        but writing to dropbox cloud only when file size less than 150M
        If not written to local nor dropbox cloud, raise an exception
        """
        upload_success = False
        if isinstance(data, str):
            data=data.encode(encoding="utf-8")
        if use_gzip:
            data_gzipped = gzip.compress(data) 
            if not rel_path.endswith("gz"):
                rel_path = "%s.gz" % rel_path
        else: 
            data_gzipped = data
        if self.use_localfs:
            local_full_path = self.local_prefix + rel_path
            with open(local_full_path, 'wb') as file:
                file.write(data_gzipped)
                logger.debug(f"Written file to local FS: {local_full_path}")
            upload_success = True    
        if self.use_dropbox:
            if len(data_gzipped) < 150_000_000:
                cloud_full_path = self._remove_doubleslash_endslash (self.cloud_prefix + rel_path)
                self.dbx_upload(data_gzipped, cloud_full_path) # will add self.cloud_prefix +  at dbx_upload
                logger.debug(f"Written file to cloud FS: {cloud_full_path}")
                upload_success = True
            if len(data_gzipped) >= 150_000_000:    
                logger.critical("File size over 150M, cannot write to Dropbox {self.cloud_prefix}{rel_path}")
        if not upload_success:
            logger.critical(f"use_localfs = False; Neither can write to dropbox {rel_path}")

    def read(self, rel_path, read_mode = "rb", use_gzip=False):
        """
        write(...)  allows writing to both local storage and dropbox cloud
        but  read() will try reading from local first if allowed; otherwise if allowed read from dropbox
        self = mylc
        rel_path = "/tmp/var1.txt.gz"
        """
        return_value = bytes()
        bytes_data, txt = bytes(), str()
        if self.use_localfs:
            with open(self.local_prefix + rel_path, 'rb') as file:
                bytes_data = file.read()  
        elif self.use_dropbox :
            dbx_full_path = self._remove_doubleslash_endslash (self.cloud_prefix + rel_path)
            bytes_data = self.dbx_download(dbx_full_path = dbx_full_path)
        else:
            logger.critical("use_localfs and use_dropbox are both False")    
        if use_gzip:
            decompressed_bytes = gzip.decompress(bytes_data)  
        else: 
            decompressed_bytes = bytes_data
        return_value =  decompressed_bytes
        if not read_mode=="rb":
            return_value = decompressed_bytes.decode()
        return return_value

    def sync_file(self, local_rel_path, cloud_rel_path, from_cloud_to_local = False):
        """This method has not been called anywhere?"""
        if self.use_localfs and self.use_dropbox:
            dbx_full_path = self._remove_doubleslash_endslash(self.cloud_prefix + cloud_rel_path)
            if from_cloud_to_local:
                self.dbx_download(dbx_full_path = dbx_full_path, local_full_path = self.local_prefix + local_rel_path )
            else: #from_local_to_cloud
                with open(self.local_prefix + local_rel_path, 'rb') as file:
                    bytes_data = file.read()  
                    self.dbx_upload(bytes_data,  dbx_full_path = dbx_full_path)
        else:
            logger.error("Cannot sync unless both local and cloud are turned on.")

    @staticmethod
    def get_existing_dropbox_token():
        """Attempt to obtain dropbox_token from 
            1. args
            2. stored file
        If neither is provided, return None
        """
        access_token = None

        if args.dropbox_access_token:
            access_token = args.dropbox_access_token  

        elif os.path.isfile("./.dropbox_access_token"):
            with open("./.dropbox_access_token", "r") as f:
                access_token = f.read()

        return access_token

    @staticmethod
    def authorize_dropbox_over_web(app_key: str, app_secret: str):
        #token_access_type (str) – the type of token to be requested. From the following enum:
        #None - creates a token with the app default (either legacy or online)
        #legacy - creates one long-lived token with no expiration
        #online - create one short-lived token with an expiration
        #offline - create one short-lived token with an expiration with a refresh token
        auth_flow = dropbox.oauth.DropboxOAuth2FlowNoRedirect(app_key, app_secret, token_access_type="offline")
        authorize_url = auth_flow.start()
        logger.info("Please authorize the app by visiting this URL:", authorize_url)
        print("Please authorize the app by visiting this URL:", authorize_url)
        auth_code = input("Enter the authorization code: ")

        oauth_result = auth_flow.finish(auth_code)
        access_token = oauth_result.access_token
        with open("./.dropbox_access_token", "wt") as f:
            f.write(access_token)
        print(f"Dropbox Account Authorization of App with app-key {app_key} completed!")    
        return access_token

    def connect_dropbox(self, access_token = None, retrying_already = False):
        """
        Setup Dropbox Connection
        You need to apply for a Dropbox APP, then get an API token from Dropbox app console
        Dropbox app console: https://www.dropbox.com/developers/apps?_tk=pilot_lp&_ad=topbar4&_camp=myapps

        To assign dropbox token: 
        * Either run in console: export DBX_TOKEN = ${your_token}
            Shortened example: export DBX_TOKEN=sl.BeoSWaH2atxxxxtm-4 
        Or include it in the parameter --

        See Dropbox Python API Documentation: https://dropbox-sdk-python.readthedocs.io/en/latest/
        """
        if access_token==None: 
            access_token = self.authorize_dropbox_over_web(self.dropbox_app_key, self.dropbox_app_secret)
        dbx = dropbox.Dropbox(access_token)
        try:
            account = dbx.users_get_current_account()
            logger.info("Connected to Dropbox successfully! ")
            #logger.info(f"Account information: {account}")
        except dropbox.exceptions.AuthError as e:
            if retrying_already:
                logger.critical("CANNOT ACCESS DROPBOX")
                dbx = None
            else:    
                dbx = self.connect_dropbox(access_token = None, retrying_already=True)
        return dbx

    
    
def create_parser():
    """Argument Parser
    This function automatically creats a HELP message if run this python code in commandline with -h or --help
    And takes the argument
    
    In the default behavior of argparse, the translation from the command-line argument to the attribute name does involve replacing dashes (-) with underscores (_).
    See documentation: https://docs.python.org/3/library/argparse.html
    """
    class CustomFormatter(argparse.HelpFormatter):
        def _split_lines(self, text, width):
            lines = super()._split_lines(text, width)
            #print("DOC "+ str(width))
            #for line in lines:
            #    print("DOC: "+ line + str(len(line)))
            #return [line if line.strip() else '' for line in lines]    
            return [line + "\n" if len(line)<width-5 else line for line in lines]    
    parser = argparse.ArgumentParser(formatter_class=CustomFormatter)
    
    # Whether to use Local FS or Dropbox Cloud, and the folder structure info needed
    parser.add_argument("-lfs", "--use-localfs", action="store_true", help="To use local filesystem storage for data read/write")
    parser.add_argument("-lroot", "--localfs-rootfolder", type=str, required=False, default= "/",
        help="Specify local filesystem data root folder")
    
    parser.add_argument("-dbx", "--use-dropbox", action="store_true", 
                        help="Use dropbox storage for data read/write")
    parser.add_argument("-croot", "--cloudfs-rootfolder", type=str, required=False, default= "/",
        help="Specify Dropbox cloud filesystem data root folder")
    
    parser.add_argument("-syimf", "--sync-if-missing-file", action="store_true", 
        help="""When a file used(read or write) is msing in one place but existing in the other two, will sync the file such that it exists in both places""")
    
    parser.add_argument("-dbxtkn", "--dropbox-access-token", type=str, required = False, help="""Dropbox Access Token. For your own Dropbox APP (applied in Dropbox Developer), generate it from Dropbox Developer App Console https://www.dropbox.com/developers/apps?_tk=pilot_lp&_ad=topbar4&_camp=myapps Otherwise need 2-Step Authorization with your Browser. Once generated, would be stored at ./.dropbox_access_token in the current directory where you invoke this python code""")
    
    parser.add_argument("-dbxkey", "--dropbox-app-key", type=str,  required = False, help="""Dropbox App Key. For your own Dropbox APP (applied in Dropbox Developer), obtain it from Dropbox Developer App Console https://www.dropbox.com/developers/apps?_tk=pilot_lp&_ad=topbar4&_camp=myapps""")
    
    parser.add_argument("-dbxsct", "--dropbox-app-secret", type=str,  required = False, help="""Dropbox App Secret. For your own Dropbox APP (applied in Dropbox Developer), obtain it from Dropbox Developer App Console https://www.dropbox.com/developers/apps?_tk=pilot_lp&_ad=topbar4&_camp=myapps""")
    
    
    # Additional Info needed
    parser.add_argument('-ua', '--http-user-agent', type=str, required=False, default= "Name of Your Institution youremail@yourinstitute.edu",
        help="""SEC EDGAR requires that bots declare your user agent in request headers:
        Sample Company Name AdminContact@<sample_company_domain>.com
        See https://www.sec.gov/os/accessing-edgar-data for more info""")
    return parser    


if __name__=="__main__": 
    
    parser = create_parser()
    
    args = parser.parse_args()    
    
    logger.debug("Beginning arguments:")
    for arg in vars(args):
        logger.debug(f"{arg}: {getattr(args, arg)}")    

    _use_localfs = False
    if args.use_localfs:
        _use_localfs = True
    
    _use_dropbox = False
    if args.use_dropbox:
        _use_dropbox = True

    _sync_if_missing_file = args.sync_if_missing_file
    if _sync_if_missing_file: 
        if not _use_localfs or not _use_dropbox:
            logger.error("--sync-if-missing-file is ON. Assume both local Filesystem and Dropbox Turned on.")    
            _use_dropbox = True
            _use_localfs = True
    
    if  _use_localfs and  _use_dropbox:
        if  _sync_if_missing_file:
            pass
        else: 
            logger.info("""Both local Filesystem and Dropbox are On, but not --sync-if-missing-file. 
            When reading data, Will first try to find file in local FS; if unfound then check Dropbox; 
            if still unfound then build dataset and save to both FSs.""")

    if not _use_localfs and not _use_dropbox:
        logger.info("Neither local Filesystem nor Dropbox Turned on. Assume local FS is on from now on")    
        _use_localfs = True
            
    mo = ManageOvercloud(use_localfs = _use_localfs, use_dropbox = _use_dropbox, 
                      local_prefix = args.localfs_rootfolder, cloud_prefix = args.cloudfs_rootfolder,
                      dropbox_app_key = args.dropbox_app_key, dropbox_app_secret = args.dropbox_app_secret,
                      sync_if_missing_file=_sync_if_missing_file)
