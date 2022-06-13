#
# © Copyright IBM Corporation 2022, 2022
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# This file contains the source code for submitting a transfer request
# to a IBM MQ Managed File Transfer agent using REST APIs and then query
# the status of transfer.
#
# The program does the following:
# 1) Builds a JSON object containing the transfer request.
# 2) Builds a HTTP POST request and then submits it to IBM MQ Web Server.
# 3) Builds a HTTP GET request with URL returned in step #2 above to query
#    the status of a transfer. If transfer status is not yet available, the
#    program waits for 5 seconds and resubmits the HTTP GET request again to
#    query the transfer status.
#
# This program assumes the following:
# 1) MFT network has been setup with at least two agents.
# 2) Basic authentication for REST APIs has been configured.
# 3) MQ Web Server has been configured and started.


import json
import http.client
import base64

# Host name of IBM MQ Web Server
mqWebServer = "localhost"
# Port number where IBM MQ Web Server is listening
mqWebServerPort = 8080
# Transfer REST API endpoint
transferRequestUrl = "/ibmmq/rest/v2/admin/mft/transfer"
# Sample user Id for authenticating to web server
mqWebUserId = "mftuser"
# Sample password for authenticating to web server
mqWebPassword = "mftpassw0rd"
# Name of the source agent
sourceAgentName  = "SRC"
# Name of the destination agent
destinationAgentName = "DEST"
# Name of the source agent queue manager
sourceQMName = "SRCQM"
# Name of the destination agent queue manager
destinationQMName = "DESTQM"
# Path of the source files
sourceItemName = "/usr/srcdir"
# Destination path to where files will be transferred to
destinationItemName = "/usr/destdir"
# Source path type
sourceItemType = "file"
# Destination path type
destinationItemType = "directory"

# Build and return a transfer request in JSON format
def buildTransferRequest(): 
    # Source agent attributes
    sourceAgentAttributes = {
        'qmgrName' : sourceQMName,
        'name':sourceAgentName
    }

    # Destination agent attributes
    destinationAgentAttributes = {
        'qmgrName': destinationQMName,
        'name': destinationAgentName
    }

    # Source item attributes
    sourceItem = {
        'name': sourceItemName,
        'type': sourceItemType
    }

    # Destination item attributes
    destinationItem = {
        'name': destinationItemName,
        'type': destinationItemType
    }
    
    # Transfer item group
    item = {
        'source':sourceItem,
        'destination':destinationItem
    }

    # Add the transfer item(s) as an array to transferSet group
    transferSet = {
        'item':[item]
    }

    # Build a complete transfer request with above attributes
    transferRequest = {
        'sourceAgent' : sourceAgentAttributes,
        'destinationAgent': destinationAgentAttributes,
        'transferSet':transferSet
    }
    # Convert as JSON string and return
    jsonStr = json.dumps(transferRequest)
    return jsonStr

# Build required headers for the HTTP request
# @params:
#    userId: User id for authentication
#    password: Password for authentication
def buildHTTPHeaders(userId, password):
    encodedUidPwd = base64.b64encode(bytes(userId + ":" + password, 'utf-8'))
    mqRESTHeaders = {"Content-type": "application/json",
                     "ibm-mq-rest-csrf-token": "", "Authorization":"Basic " + str(encodedUidPwd)}
    return mqRESTHeaders

# Submit transfer request using HTTP POST verb
# @params:
#    url: transfer REST endpoint
#    body: Transfer request json
#    userId: User id for authentication
#    password: Password for authentication
def doPostTransfer(url , body , userId, password) :
    mqrestConn = http.client.HTTPConnection(mqWebServer, mqWebServerPort)
    mqrestConn.request("POST", url, body, buildHTTPHeaders(userId, password))
    print("Posted transfer request successfully")
    response = mqrestConn.getresponse()
    if response.status == http.client.ACCEPTED:
        print("Transfer requested accepted. Status URL " + response.getheader("location"))
        return response.getheader("location")
    else:
        print("An error occurred while posting transfer request. HTTP response code: " + str(response.getcode()))
    return response.status

# Query the status of a transfer
# @params:
#  url: Transfer URL returned by a successful transfer request submission
#  userId: UserId for authentication
#  password: Password for authentication
def doGetTransferStatus(url ,userId, password) :
    mqrestConn = http.client.HTTPConnection(mqWebServer, mqWebServerPort)
    mqrestConn.request("GET", url, "", buildHTTPHeaders(userId, password))
    response = mqrestConn.getresponse()
    if response.status != http.client.OK:
        print("Response received for GET request: " + response.status)
        return response.status
    else:
        # Read the response body
        resposeBody = response.read(response.length)
        transferStatus = json.loads(resposeBody)
        transferId = transferStatus['transfer'][0]['id']
        state = transferStatus['transfer'][0]['status']['state']
        print("Status of transfer request with Id " + transferId + " is " + state)
        if state == 'failed' :
            if 'transferSet' in transferStatus['transfer'][0]:
                transferSetItems = transferStatus['transfer'][0]['transferSet']['item']
                for item in transferSetItems:
                    if item['status']['state'] == 'failed':
                        print("Error: " + item['status']['description'])
    # End of function
    return response.status

# Build a transfer json request
transferJson = buildTransferRequest()

# Send the transfer request as the body of HTTP POST verb. If request is successful, the return
# value of the function will be a URL pointing to the status of transfer.
try:
    postResponse = doPostTransfer(transferRequestUrl, transferJson, mqWebUserId, mqWebPassword)
    if type(postResponse) == str :
        # Query the status of transfer with given URL with HTTP GET verb
        getStatus = doGetTransferStatus(postResponse + "?attributes=*", mqWebUserId, mqWebPassword)
        # If status code is not 404, then transfer may not have been started. Wait and resubmit the transfer status
        # request again.
        if getStatus == http.client.NOT_FOUND :
            doGetTransferStatus(postResponse + "?attributes=*", mqWebUserId, mqWebPassword)
except Exception as e:
    # Print the exception that was thrown
    print(e)
