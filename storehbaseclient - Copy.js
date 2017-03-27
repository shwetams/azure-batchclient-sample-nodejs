// Initializing node.js modules

var batch = require('azure-batch');
var https = require('https');
var sleep = require('thread-sleep');
var fs = require('fs');

var accountName = '<azure-batch-account-name>';
var accountKey = '<account-key-downloaded>';
var accountUrl = '<account-url>'

 

///### Delete before publishing

accountName = 'gecqrsgbtch';

accountKey = 'JF5eLlEMWjMkLiI6QGWVMiasOsGU4Tc0YNltu//Dy9sgPl0uos7/OUc44YAuaB9b8I6E5gBqWvlC22opTfrgrg==';

accountKey = 'https://gecqrsgbtch.westus.batch.azure.com'

///### Delete before publishing

// Create Batch credentials object using account name and account key
 var credentials = new batch.SharedKeyCredentials(accountName,accountKey);    

 // Create Batch service client

var batch_client = new batch.ServiceClient(credentials,accountUrl);

var now = new Date(year, month, day, hour, minute);


var poolid = "processcsv_"+ now.toDateString();
var imgRef = {publisher:"Canonical",offer:"UbuntuServer",sku:"14.04.2-LTS",version:"latest"}
var vmconfig = {imageReference:imgRef,nodeAgentSKUId:"batch.node.ubuntu 14.04"}
var numVms = 4;
var vmSize = "STANDARD_F4";

var poolConfig = {id:poolid, displayName:poolid,vmSize:vmSize,virtualMachineConfiguration:vmconfig,targetDedicated:numVms,enableAutoScale:false }

var pool = batch_client.pool.add(poolConfig,function(error,result){            
            console.log(error);
        });


var isPoolStateActive = false;
var poolCreationError = false;
var maxRetry = 10;
var numRetries = 0;
while(isPoolStateActive == false && poolCreationError == false && numRetries < maxRetry)
{
    var cloudPool = batch_client.pool.get(poolid,function(error,result){
        if(error)
        {
            console.log(error);
            poolCreationError = true;
        }
        else
        {
            if(result.state == "active")
            {
                isPoolStateActive = true;
            }
        }
    });
    setTimeout(function() {
    console.log('Waiting for Pool status...');
}, 30000);
  numRetries++;
}

if(isPoolStateActive)
{
    console.log("Pool created successfully...");
}

