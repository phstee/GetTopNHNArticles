#Implementation for HN titles based on MSFT python sdk tutorial sample
# Input: python file.py numNodes startDate endDate topN
# Splits job between N nodes with each node responsible for an even share of a chunk between start and end dates
# Uses azure batch and storage services for splitting up work 

#
# 
# python_tutorial_client.py - Batch Python SDK tutorial sample
#
# Copyright (c) Microsoft Corporation
#
# All rights reserved.
#
# MIT License
#
# Permission is hereby granted, free of charge, to any person obtaining a
# copy of this software and associated documentation files (the "Software"),
# to deal in the Software without restriction, including without limitation
# the rights to use, copy, modify, merge, publish, distribute, sublicense,
# and/or sell copies of the Software, and to permit persons to whom the
# Software is furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED *AS IS*, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
# FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
# DEALINGS IN THE SOFTWARE.

from __future__ import print_function
import datetime
import os
import sys
import time
import math

try:
    input = raw_input
except NameError:
    pass

import azure.storage.blob as azureblob
import azure.batch.batch_service_client as batch
import azure.batch.batch_auth as batchauth
import azure.batch.models as batchmodels

sys.path.append('.')
sys.path.append('..')
import helpers  # noqa
from credFile import batchName, batchKey, batchUrl, storageName, storageKey

# Update the Batch and Storage account credential strings below with the values
# unique to your accounts. These are used when constructing connection strings
# for the Batch and Storage client objects.
_BATCH_ACCOUNT_NAME = batchName
_BATCH_ACCOUNT_KEY = batchKey
_BATCH_ACCOUNT_URL = batchUrl

_STORAGE_ACCOUNT_NAME = storageName
_STORAGE_ACCOUNT_KEY = storageKey

_POOL_ID = 'GetHNTitles'
_POOL_NODE_COUNT = int(sys.argv[1]) #number of nodes is first param after filename
_POOL_VM_SIZE = 'Standard_D2_v2'
_NODE_OS_PUBLISHER = 'Canonical'
_NODE_OS_OFFER = 'UbuntuServer'
_NODE_OS_SKU = '14'

_JOB_ID = 'GetHNTitlesJob'


def query_yes_no(question, default="yes"):
    """
    Prompts the user for yes/no input, displaying the specified question text.

    :param str question: The text of the prompt for input.
    :param str default: The default if the user hits <ENTER>. Acceptable values
    are 'yes', 'no', and None.
    :rtype: str
    :return: 'yes' or 'no'
    """
    valid = {'y': 'yes', 'n': 'no'}
    if default is None:
        prompt = ' [y/n] '
    elif default == 'yes':
        prompt = ' [Y/n] '
    elif default == 'no':
        prompt = ' [y/N] '
    else:
        raise ValueError("Invalid default answer: '{}'".format(default))

    while 1:
        choice = input(question + prompt).lower()
        if default and not choice:
            return default
        try:
            return valid[choice[0]]
        except (KeyError, IndexError):
            print("Please respond with 'yes' or 'no' (or 'y' or 'n').\n")


def print_batch_exception(batch_exception):
    """
    Prints the contents of the specified Batch exception.

    :param batch_exception:
    """
    print('-------------------------------------------')
    print('Exception encountered:')
    if batch_exception.error and \
            batch_exception.error.message and \
            batch_exception.error.message.value:
        print(batch_exception.error.message.value)
        if batch_exception.error.values:
            print()
            for mesg in batch_exception.error.values:
                print('{}:\t{}'.format(mesg.key, mesg.value))
    print('-------------------------------------------')


def upload_file_to_container(block_blob_client, container_name, file_path):
    """
    Uploads a local file to an Azure Blob storage container.

    :param block_blob_client: A blob service client.
    :type block_blob_client: `azure.storage.blob.BlockBlobService`
    :param str container_name: The name of the Azure Blob storage container.
    :param str file_path: The local path to the file.
    :rtype: `azure.batch.models.ResourceFile`
    :return: A ResourceFile initialized with a SAS URL appropriate for Batch
    tasks.
    """
    blob_name = os.path.basename(file_path)

    print('Uploading file {} to container [{}]...'.format(file_path,
                                                          container_name))

    block_blob_client.create_blob_from_path(container_name,
                                            blob_name,
                                            file_path)

    sas_token = block_blob_client.generate_blob_shared_access_signature(
        container_name,
        blob_name,
        permission=azureblob.BlobPermissions.READ,
        expiry=datetime.datetime.utcnow() + datetime.timedelta(hours=2))

    sas_url = block_blob_client.make_blob_url(container_name,
                                              blob_name,
                                              sas_token=sas_token)

    return batchmodels.ResourceFile(file_path=blob_name,
                                    blob_source=sas_url)


def get_container_sas_token(block_blob_client,
                            container_name, blob_permissions):
    """
    Obtains a shared access signature granting the specified permissions to the
    container.

    :param block_blob_client: A blob service client.
    :type block_blob_client: `azure.storage.blob.BlockBlobService`
    :param str container_name: The name of the Azure Blob storage container.
    :param BlobPermissions blob_permissions:
    :rtype: str
    :return: A SAS token granting the specified permissions to the container.
    """
    # Obtain the SAS token for the container, setting the expiry time and
    # permissions. In this case, no start time is specified, so the shared
    # access signature becomes valid immediately.
    container_sas_token = \
        block_blob_client.generate_container_shared_access_signature(
            container_name,
            permission=blob_permissions,
            expiry=datetime.datetime.utcnow() + datetime.timedelta(hours=2))

    return container_sas_token


def create_pool(batch_service_client, pool_id,
                resource_files, publisher, offer, sku):
    """
    Creates a pool of compute nodes with the specified OS settings.

    :param batch_service_client: A Batch service client.
    :type batch_service_client: `azure.batch.BatchServiceClient`
    :param str pool_id: An ID for the new pool.
    :param list resource_files: A collection of resource files for the pool's
    start task.
    :param str publisher: Marketplace image publisher
    :param str offer: Marketplace image offer
    :param str sku: Marketplace image sku
    """
    print('Creating pool [{}]...'.format(pool_id))

    # Create a new pool of Linux compute nodes using an Azure Virtual Machines
    # Marketplace image. For more information about creating pools of Linux
    # nodes, see:
    # https://azure.microsoft.com/documentation/articles/batch-linux-nodes/

    # Specify the commands for the pool's start task. The start task is run
    # on each node as it joins the pool, and when it's rebooted or re-imaged.
    # We use the start task to prep the node for running our task script.
    task_commands = [
        # Copy the python_tutorial_task.py script to the "shared" directory
        # that all tasks that run on the node have access to.
        'cp -r $AZ_BATCH_TASK_WORKING_DIR/* $AZ_BATCH_NODE_SHARED_DIR',
        # Install pip and the dependencies for cryptography
        'apt-get update',
        'apt-get -y install python-pip',
        'apt-get -y install build-essential libssl-dev libffi-dev python-dev',
        # Install the azure-storage module so that the task script can access
        # Azure Blob storage
        'pip install azure-storage',
	# Install the hacker news api 
	'pip install haxor']

    # Get the node agent SKU and image reference for the virtual machine
    # configuration.
    # For more information about the virtual machine configuration, see:
    # https://azure.microsoft.com/documentation/articles/batch-linux-nodes/
    sku_to_use, image_ref_to_use = \
        common.helpers.select_latest_verified_vm_image_with_node_agent_sku(
            batch_service_client, publisher, offer, sku)

    new_pool = batch.models.PoolAddParameter(
        id=pool_id,
        virtual_machine_configuration=batchmodels.VirtualMachineConfiguration(
            image_reference=image_ref_to_use,
            node_agent_sku_id=sku_to_use),
        vm_size=_POOL_VM_SIZE,
        target_dedicated=_POOL_NODE_COUNT,
        start_task=batch.models.StartTask(
            command_line=common.helpers.wrap_commands_in_shell('linux',
                                                               task_commands),
            run_elevated=True,
            wait_for_success=True,
            resource_files=resource_files),
    )

    try:
        batch_service_client.pool.add(new_pool)
    except batchmodels.batch_error.BatchErrorException as err:
        print_batch_exception(err)
        raise


def create_job(batch_service_client, job_id, pool_id):
    """
    Creates a job with the specified ID, associated with the specified pool.

    :param batch_service_client: A Batch service client.
    :type batch_service_client: `azure.batch.BatchServiceClient`
    :param str job_id: The ID for the job.
    :param str pool_id: The ID for the pool.
    """
    print('Creating job [{}]...'.format(job_id))

    job = batch.models.JobAddParameter(
        job_id,
        batch.models.PoolInformation(pool_id=pool_id))

    try:
        batch_service_client.job.add(job)
    except batchmodels.batch_error.BatchErrorException as err:
        print_batch_exception(err)
        raise


def add_tasks(batch_service_client, job_id, minDate, maxDate, topN,
              output_container_name, output_container_sas_token):
    """
    Splits span between minDate and maxDate into _POOL_NODE_COUNT chunks
	then creates a task for each chunk.

    min and max date inputs are datetimes

    :param batch_service_client: A Batch service client.
    :type batch_service_client: `azure.batch.BatchServiceClient`
    :param list input_files: A collection of input files. One task will be
    :param output_container_name: The ID of an Azure Blob storage container to
    which the tasks will upload their results.
    :param output_container_sas_token: A SAS token granting write access to
    the specified Azure Blob storage container.
    """

    #create an array of _POOL_NODE_COUNT number tuples with (startDate,endDate) that cover the range between minDate and maxDate
    chunkSize = math.ceil(((maxDate-minDate).days + 1)/float(_POOL_NODE_COUNT)) #add one to day delta then get ceiling to ensure chunk sizes are as large as possible; last chunk can be undersized 
 
    chunks = []

    #for each node, designate a range of non-overlapping dates into the chunks array 
    for i in range(_POOL_NODE_COUNT):
	startChunk = minDate + datetime.timedelta(days=(i*chunkSize))
	endChunk = minDate + datetime.timedelta(days=((i+1)*chunkSize - 1))

	if endChunk > maxDate:
		endChunk = maxDate

	chunks.append((startChunk, endChunk))
    

    print('Adding {} tasks to job [{}]...'.format(_POOL_NODE_COUNT, job_id)) #adding a task for each node 

    tasks = list()

    idx = 0
    for timeRange in chunks:

	#print "Creating a task for time range " + str(timeRange[0]) + " to " + str(timeRange[1]) #debug

        command = ['python $AZ_BATCH_NODE_SHARED_DIR/GetHNTitles-Batch.py --topN {} --minDate "{}" --maxDate "{}" --storageaccount {} --storagecontainer {} --sastoken "{}"'.format(
		       str(topN),
		       str(timeRange[0].isoformat()), #outputs datetime as yyyy-mm-dd
		       str(timeRange[1].isoformat()),
                       _STORAGE_ACCOUNT_NAME,
                       output_container_name,
                       output_container_sas_token)]

        tasks.append(batch.models.TaskAddParameter(
			'hnTask{}'.format(idx), 
		        helpers.wrap_commands_in_shell('linux', command)))

	idx = idx + 1


    #debug
    for task in tasks:
	print('Task: {}'.format(task))


    batch_service_client.task.add_collection(job_id, tasks)


def wait_for_tasks_to_complete(batch_service_client, job_id, timeout):
    """
    Returns when all tasks in the specified job reach the Completed state.

    :param batch_service_client: A Batch service client.
    :type batch_service_client: `azure.batch.BatchServiceClient`
    :param str job_id: The id of the job whose tasks should be to monitored.
    :param timedelta timeout: The duration to wait for task completion. If all
    tasks in the specified job do not reach Completed state within this time
    period, an exception will be raised.
    """
    timeout_expiration = datetime.datetime.now() + timeout

    print("Monitoring all tasks for 'Completed' state, timeout in {}..."
          .format(timeout), end='')

    while datetime.datetime.now() < timeout_expiration:
        print('.', end='')
        sys.stdout.flush()
        tasks = batch_service_client.task.list(job_id)

        incomplete_tasks = [task for task in tasks if
                            task.state != batchmodels.TaskState.completed]
        if not incomplete_tasks:
            print()
            return True
        else:
            time.sleep(1)

    print()
    raise RuntimeError("ERROR: Tasks did not reach 'Completed' state within "
                       "timeout period of " + str(timeout))


def download_blobs_from_container(block_blob_client,
                                  container_name, directory_path):
    """
    Downloads all blobs from the specified Azure Blob storage container.

    :param block_blob_client: A blob service client.
    :type block_blob_client: `azure.storage.blob.BlockBlobService`
    :param container_name: The Azure Blob storage container from which to
     download files.
    :param directory_path: The local directory to which to download the files.
    """
    print('Downloading all files from container [{}]...'.format(
        container_name))

    container_blobs = block_blob_client.list_blobs(container_name)

    for blob in container_blobs.items:
        destination_file_path = os.path.join(directory_path, blob.name)

        block_blob_client.get_blob_to_path(container_name,
                                           blob.name,
                                           destination_file_path)

        print('  Downloaded blob [{}] from container [{}] to {}'.format(
            blob.name,
            container_name,
            destination_file_path))

    print('  Download complete!')


if __name__ == '__main__':

    #read in top N, start and end dates from cmd line
    #_POOL_NODE_COUNT = sys.argv[1] is declared in top goo 
    minDateStr = sys.argv[2] 
    maxDateStr = sys.argv[3]
    topN = int(sys.argv[4])

    minDate = datetime.datetime.strptime(minDateStr, "%m/%d/%Y").date()
    maxDate = datetime.datetime.strptime(maxDateStr, "%m/%d/%Y").date()

    #main code

    start_time = datetime.datetime.now().replace(microsecond=0)
    print('Sample start: {}'.format(start_time))
    print()

    # Create the blob client, for use in obtaining references to
    # blob storage containers and uploading files to containers.
    blob_client = azureblob.BlockBlobService(
        account_name=_STORAGE_ACCOUNT_NAME,
        account_key=_STORAGE_ACCOUNT_KEY)

    # Use the blob client to create the containers in Azure Storage if they
    # don't yet exist.
    app_container_name = 'application'
    output_container_name = 'output'
    blob_client.create_container(app_container_name, fail_on_exist=False)
    blob_client.create_container(output_container_name, fail_on_exist=False)

    # Paths to the task script. This script will be executed by the tasks that
    # run on the compute nodes.
    application_file_paths = [os.path.realpath('GetHNTitles-Batch.py')]


    # Upload the application script to Azure Storage. This is the script that
    # will process the data files, and is executed by each of the tasks on the
    # compute nodes.
    application_files = [
        upload_file_to_container(blob_client, app_container_name, file_path)
        for file_path in application_file_paths]


    # Obtain a shared access signature that provides write access to the output
    # container to which the tasks will upload their output.
    output_container_sas_token = get_container_sas_token(
        blob_client,
        output_container_name,
        azureblob.BlobPermissions.WRITE)

    # Create a Batch service client. We'll now be interacting with the Batch
    # service in addition to Storage
    credentials = batchauth.SharedKeyCredentials(_BATCH_ACCOUNT_NAME,
                                                 _BATCH_ACCOUNT_KEY)

    batch_client = batch.BatchServiceClient(
        credentials,
        base_url=_BATCH_ACCOUNT_URL)

    # Create the pool that will contain the compute nodes that will execute the
    # tasks. The resource files we pass in are used for configuring the pool's
    # start task, which is executed each time a node first joins the pool (or
    # is rebooted or re-imaged).
    create_pool(batch_client,
                _POOL_ID,
                application_files,
                _NODE_OS_PUBLISHER,
                _NODE_OS_OFFER,
                _NODE_OS_SKU)

    # Create the job that will run the tasks.
    create_job(batch_client, _JOB_ID, _POOL_ID)

    # Add the tasks to the job. We need to supply a container shared access
    # signature (SAS) token for the tasks so that they can upload their output
    # to Azure Storage.
    add_tasks(batch_client,
              _JOB_ID,
              minDate,
	      maxDate,
	      topN,
              output_container_name,
              output_container_sas_token)

    # Pause execution until tasks reach Completed state.
    wait_for_tasks_to_complete(batch_client,
                               _JOB_ID,
                               datetime.timedelta(minutes=60))

    print("  Success! All tasks reached the 'Completed' state within the "
          "specified timeout period.")

    # Download the task output files from the output Storage container to a
    # local directory
    download_blobs_from_container(blob_client,
                                  output_container_name,
                                  os.path.expanduser('~'))

    # Clean up storage resources
    print('Deleting containers...')
    blob_client.delete_container(app_container_name)
    blob_client.delete_container(output_container_name)

    # Print out some timing info
    end_time = datetime.datetime.now().replace(microsecond=0)
    print()
    print('Sample end: {}'.format(end_time))
    print('Elapsed time: {}'.format(end_time - start_time))
    print()

    # Clean up Batch resources (if the user so chooses).
    if query_yes_no('Delete job?') == 'yes':
        batch_client.job.delete(_JOB_ID)

    if query_yes_no('Delete pool?') == 'yes':
        batch_client.pool.delete(_POOL_ID)

    print()
    input('Press ENTER to exit...')
