// Initializing node.js modules

var batch = require('azure-batch');
var https = require('https');
//var sleep = require('thread-sleep');
var fs = require('fs');
var uuid = require('node-uuid');

var accountName = '<azure-batch-account-name>';
var accountKey = '<account-key-downloaded>';
var accountUrl = '<account-url>'

 

///### Delete before publishing

accountName = 'batchdevsg';

accountKey = 'lJiQHyntVBnt73cRv9WJrmYVakdO5JOvFz/ovW70DpFm/DL3AJQkPRWkx9neANG6ars2z6RbOwYU3O6NaS2V5Q==';

accountUrl = 'https://batchdevsg.centralus.batch.azure.com'

///### Delete before publishing

// Create Batch credentials object using account name and account key
 var credentials = new batch.SharedKeyCredentials(accountName,accountKey);    

 // Create Batch service client

var batch_client = new batch.ServiceClient(credentials,accountUrl);

var now = new Date();


var poolid = "processcsv_"+ now.getFullYear() + now.getMonth() + now.getDay() + now.getHours() + now.getSeconds()

console.log("Creating pool with ID" + poolid);

var imgRef = {publisher:"Canonical",offer:"UbuntuServer",sku:"14.04.2-LTS",version:"latest"}
var vmconfig = {imageReference:imgRef,nodeAgentSKUId:"batch.node.ubuntu 14.04"}
var numVms = 4;
var vmSize = "STANDARD_A1";

var clientPoolRequestID = uuid.v4();
var poolConfig = {id:poolid, displayName:poolid,vmSize:vmSize,virtualMachineConfiguration:vmconfig,targetDedicated:numVms,enableAutoScale:false }
var poolAddOptionsConfig = {clientRequestId:clientPoolRequestID,returnClientRequestId:true}
var poolOptions = {poolAddOptions:poolAddOptionsConfig}
var pool = batch_client.pool.add(poolConfig,poolOptions,function(error,result,request,response){            
    //        console.log("Pool add response stream" + response.body);
            if(error != null)
            {
                console.log(error.response);
            }
            
        });

console.log("Submitted request to create pool...");


var isPoolStateActive = false;
var poolCreationError = false;
var maxRetry = 1000;
var numRetries = 0;
var jobCreated = false;

while(isPoolStateActive == false && poolCreationError == false && numRetries < maxRetry)
{
    
    var cloudPool = batch_client.pool.get(poolid,function(error,result,request,response){
        if(error == null)
        {
            //console.log(result);
            //console.log("Result state : " + result.state);
            if(result.state == "active")
            {
                  isPoolStateActive = true;
                  if(jobCreated==true)
                  {
                      // do nothing
                  }
                  else
                  {
                     jobCreated = true;
                     console.log("Pool created successfully now submitting job");
                  }
                  //// Create the Job 
            }
        }
        else
        {
            if(error.statusCode==404)
            {
                console.log("Pool not found yet returned 404...");    
                           
            }
            else
            {
                poolCreationError = true;
            }
        }
        });
  
  numRetries++;
 
}

console.log("Pool Status : " + isPoolStateActive);

if(isPoolStateActive == true)
{
    

}
else
{
    console.log("Failed to get pool status ....");
}




function createJob()
{
    console.log("Pool created successfully...");
    var sh_url = "https://batchdevsgsa.blob.core.windows.net/downloads/startup_prereq.sh?st=2017-03-27T10%3A53%3A00Z&se=2017-03-28T10%3A53%3A00Z&sp=rl&sv=2015-12-11&sr=b&sig=3JgXBwhJArpwZSSxakOFAeQhL%2BIXv5ivdbcAS84Fnko%3D";
    var job_prep_task_config = {id:"installprereq",commandLine:"sudo sh startup_prereq.sh > startup.log",resourceFiles:[{'blobSource':sh_url,'filePath':'startup_prereq.sh'}],waitForSuccess:true,runElevated:true}
    // Setting up Batch pool configuration
    var poolConfigJob = {poolId:poolid}
    
    // Setting up Job configuration along with preparation task
    var jobId = "processcsvjob";
    var job_config = {id:jobId,displayName:"process csv files",jobPreparationTask:job_prep_task_config,poolInfo:pool_config}
    
     // Adding Azure batch job to the pool
     var job = batch_client.job.add(job_config,function(error,result){
        if(error !=null)
        {
            console.log(error);
        }        
        
    });       
    var maxRetryJob = 100;
    var isJobCreated = false
    var isJobCreatedError = false
    var numRetriesJob = 0;
    while(numRetriesJob < maxRetryJob && isJobCreated == false && isJobCreatedError == false)
    {
        var jobStatus = batch_client.job.get(jobId,function(error,result,request,response)
        {
            if(error == null)
            {
                console.log(result);
                console.log("Result state" + result.state );
                if(result.state == 'active')
                {
                      isJobCreated = true;  
                }
            }
            else
            {
                if(error.statusCode == 404)
                {
                    console.log("Job not created yet, received 404...");
                }
                else
                {
                    console.log("Error occured while job creation" + error.response);
                    isJobCreatedError = true;
                }
            }
        }); 
        numRetriesJob++;
   }
    if(isJobCreated)
    {

        console.log("Job has been created");
    }

}

function createTasks()
{
    var container_list = ["con1","con2","con3","con4"]
    container_list.forEach(function(val,index){
           var date_param = Math.floor((new Date()).getUTCMilliseconds() / 1000)
           var exec_info_config = {startTime: date_param}
           var container_name = val;
           var taskID = container_name + "_process";
           var task_config = {id:taskID,displayName:'process csv in ' + container_name,commandLine:'python processcsv.py --container ' + container_name,resourceFiles:[{'blobSource':'blob SAS URI','filePath':'processcsv.py'}]}
           var task = batch_client.task.add(poolid,task_config,function(error,result){
                if(error != null)
                {
                    console.log(error.response);     
                }
                else
                {
                    console.log("Task for container : " + container_name + "submitted successfully");
                }
               
               

           });

    });
       
}
