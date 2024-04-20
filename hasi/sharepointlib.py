# -*- coding: utf8 -*-
###############################################################################
#                               sharepointlib.py                              #
###############################################################################
# Copyright (C) 2023  Jens Janzen

# Change history
# V1.0: 2019-04-26 - Initial Release

# Description:
# Library contains helper functions to communicate with Microsoft Sharepoint.
import json
import requests
import time

standard_encoding = 'utf8'


class SharepointObject(object):
    """
    Class for communication with Microsoft Sharepoint
    """

    def __init__(self, sp_resource, tenant_domain, tenant_id, sp_token_endpoint, sp_site_url, client_id, client_secret):
        self.sp_resource = sp_resource
        self.tenant_domain = tenant_domain
        self.tenant_id = tenant_id
        self.sp_token_endpoint = sp_token_endpoint
        self.sp_site_url = sp_site_url
        self.client_id = client_id
        self.client_secret = client_secret
        self.token = None

    def get_token(self):
        resource = "{0}/{1}@{2}".format(self.sp_resource, self.tenant_domain, self.tenant_id)
        token_endpoint = "{0}/{1}/tokens/OAuth/2".format(self.sp_token_endpoint, self.tenant_id)
        headers = {"Content-Type": "application/x-www-form-urlencoded"}
        scope = "Web.Read List.Write"
        request_params = {'client_id': "{0}@{1}".format(self.client_id, self.tenant_id), 'scope': scope,
                          'client_secret': self.client_secret, 'grant_type': 'client_credentials', 'resource': resource}
        print("{0}: Request token from {1}\nwith client id {2} for scope {3}.".format(
            time.strftime("%Y-%m-%d %H.%M.%S"), token_endpoint, self.client_id, scope))
        accesstokenrequest = requests.post(token_endpoint, data=request_params, headers=headers)
        accesstokenresponse = accesstokenrequest.json()
        token = accesstokenresponse["access_token"]
        return token

    def upload_to_sharepoint(self, target_directory, folder_path, file_name):
        self.token = self.get_token()
        site_url = "https://{0}/{1}".format(self.tenant_domain, self.sp_site_url)
        file_object = "{0}/{1}".format(target_directory, file_name)
        print("{0}: Create authorization headers.".format(time.strftime("%Y-%m-%d %H.%M.%S")))
        auth_headers = {"Authorization": "Bearer {0}".format(self.token)}
        url = "{0}/_api/web/GetFolderByServerRelativeUrl('{1}')/Files/add(url='{2}',overwrite=true)".format(
            site_url, folder_path, file_name)
        print("{0}: Upload file object {1}\nto site {2}.".format(time.strftime("%Y-%m-%d %H.%M.%S"),
                                                                 file_object, site_url))
        r = requests.post(url, data=open(file_object, "rb"), headers=auth_headers)
        if r.status_code == requests.codes.ok:
            print("{0}: Upload of {1} successful. Return code {2}.".format(
                time.strftime("%Y-%m-%d %H.%M.%S"), file_object, r.status_code))
            return True
        else:
            print("{0}: Error {1} occurred during upload of file {2}.".format(
                time.strftime("%Y-%m-%d %H.%M.%S"), r.status_code, file_object))
            print(r.text)
            return False

    def download_from_sharepoint(self, folder_path, file_name):
        self.token = self.get_token()
        site_url = "https://{0}/{1}".format(self.tenant_domain, self.sp_site_url)
        print("{0}: Create authorization headers.".format(time.strftime("%Y-%m-%d %H.%M.%S")))
        auth_headers = {"Authorization": "Bearer {0}".format(self.token),
                        "accept": "application/json;odata=verbose"}
        url = "{0}/_api/web/GetFolderByServerRelativeUrl('{1}')/Files('{2}')/$value".format(
            site_url, folder_path, file_name)
        print("{0}: Show folder: {1}/{2}.".format(time.strftime("%Y-%m-%d %H.%M.%S"), site_url, folder_path))
        r = requests.get(url, headers=auth_headers)
        if r.status_code == requests.codes.ok:
            return r
        else:
            print("{0}: Error {1} occurred.".format(time.strftime("%Y-%m-%d %H.%M.%S"), r.status_code))
            return r

    def create_sp_folder(self, folder_path, folder_name):
        self.token = self.get_token()
        site_url = "https://{0}/{1}".format(self.tenant_domain, self.sp_site_url)
        print("{0}: Create authorization headers.".format(time.strftime("%Y-%m-%d %H.%M.%S")))
        auth_headers = {"Authorization": "Bearer {0}".format(self.token),
                        "X-RequestDigest": "form digest value",
                        "accept": "application/json;odata=verbose",
                        "content-type": "application/json;odata=verbose",
                        "content-length": "length of post body"}

        url = "{0}/_api/web/Folders".format(site_url)
        payload = {"__metadata": {"type": "SP.Folder"}, "ServerRelativeUrl": "{0}/{1}".format(folder_path, folder_name)}
        r = requests.post(url, data=json.dumps(payload), headers=auth_headers)
        print("Create folder {0}/{1}/{2}".format(site_url, folder_path, folder_name))
        if r.status_code == 201:
            print("{0}: Folder {1} created successfully. Return code {2}.".format(
                time.strftime("%Y-%m-%d %H.%M.%S"), folder_name, r.status_code))
            return True
        else:
            print("{0}: Error {1} occurred during folder creation {2}/{3}.".format(
                time.strftime("%Y-%m-%d %H.%M.%S"), r.status_code, folder_path, folder_name))
            return False

    def check_sp_folder(self, folder_path, folder_name):
        self.token = self.get_token()
        site_url = "https://{0}/{1}".format(self.tenant_domain, self.sp_site_url)
        print("{0}: Create authorization headers.".format(time.strftime("%Y-%m-%d %H.%M.%S")))
        auth_headers = {"Authorization": "Bearer {0}".format(self.token),
                        "X-HTTP-Method": "GET",
                        "accept": "application/json;odata=verbose"}

        url = "{0}/_api/web/GetFolderByServerRelativeUrl('{1}/{2}')".format(site_url, folder_path, folder_name)
        r = requests.post(url, headers=auth_headers)
        print("Check folder {0}/{1}/{2}".format(site_url, folder_path, folder_name))
        if r.status_code == 200:
            return True
        elif r.status_code != 200:
            return False
