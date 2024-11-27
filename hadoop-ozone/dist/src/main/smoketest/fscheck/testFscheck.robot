# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

*** Settings ***
Documentation       Smoketest ozone cluster startup with corrupted key test
Library             OperatingSystem
Library             String
Library             Collections
Library             Process
Resource            ../commonlib.robot
Resource            ../ozone-lib/shell.robot

*** Variables ***
${TEST_VOLUME}            volume1
${TEST_BUCKET}            bucket1

*** Test Cases ***
Test FSCheck Healthy Key
    [Setup]    Setup Ozone Test Environment         1
    Run FSCheck With Verbose Key                    1
    Run FSCheck With Verbose Chunk                  1
    Run FSCheck With Verbose Block
    Run FSCheck With Verbose Container
    Delete Chunks                                   1
    [Teardown]    Cleanup Ozone Test Environment    1

Test FSCheck Multiple Healthy Keys
    [Setup]    Setup Ozone Test Environment         3
    Run FSCheck With Verbose Key                    3
    Run FSCheck With Verbose Chunk                  3
    Run FSCheck With Verbose Block
    Run FSCheck With Verbose Container
    Delete Chunks                                   3
    [Teardown]    Cleanup Ozone Test Environment    3

Test FSCheck Healthy And Unhealthy Key
    [Setup]    Setup Ozone Test Environment         2
    Corrupt Chunks                                  1
    Run FSCheck Damaged Keys                        1
    Delete Chunks                                   1
    [Teardown]    Cleanup Ozone Test Environment    2

Test FSCheck Multiple Keys One Corrupted Key
    [Setup]    Setup Ozone Test Environment         3
    Corrupt Chunks                                  1
    Run FSCheck With Verbose Key                    2
    Run FSCheck With Verbose Chunk                  2
    Run FSCheck With Verbose Block
    Run FSCheck With Verbose Container
    Run FSCheck Damaged Keys                        1
    Delete Chunks                                   1
    [Teardown]    Cleanup Ozone Test Environment    3

Test FSCheck Multiple Keys One Deleted Chunk
    [Setup]    Setup Ozone Test Environment         3
    Delete Chunks                                   1
    Run FSCheck With Verbose Key                    2
    Run FSCheck With Verbose Chunk                  2
    Run FSCheck With Verbose Block
    Run FSCheck With Verbose Container
    Run FSCheck Damaged Keys                        1
    [Teardown]    Cleanup Ozone Test Environment    3

Test FSCheck Multiple Keys Several Corrupted
    [Setup]    Setup Ozone Test Environment         4
    Corrupt Chunks                                  2
    Run FSCheck Damaged Keys                        2
    Run FSCheck With Verbose Key                    2
    Run FSCheck With Verbose Chunk                  2
    Run FSCheck With Verbose Block
    Run FSCheck With Verbose Container
    Delete Chunks                                   1
    [Teardown]    Cleanup Ozone Test Environment    4

Test FSCheck Multiple Keys All Corrupted
    [Setup]    Setup Ozone Test Environment         3
    Corrupt Chunks                                  3
    Run FSCheck Damaged Keys                        3
    [Teardown]    Cleanup Ozone Test Environment    3

Test FSCheck Multiple Keys All Chunks Deleted
    [Setup]    Setup Ozone Test Environment         3
    Delete Chunks                                   3
    Run FSCheck Damaged Keys                        3
    [Teardown]    Cleanup Ozone Test Environment    3

Test FSCheck Corrupted Key Deletion
    [Setup]    Setup Ozone Test Environment         1
    Corrupt Chunks                                  1
    Run FSCheck With Delete Option
    [Teardown]    Cleanup Ozone Test Environment    1

*** Keywords ***
Setup Ozone Test Environment
    [Arguments]    ${num_keys}
    Log    Setting up Ozone test environment...
    Execute And Ignore Error    ozone sh volume create ${TEST_VOLUME}
    Execute And Ignore Error    ozone sh bucket create --type=${TYPE} --replication=${REPLICATION} ${TEST_VOLUME}/${TEST_BUCKET}
    ${keys}=    Evaluate    ${num_keys} + 1
    FOR    ${i}    IN RANGE    1    ${keys}
        ${key}    Set Variable    key${i}
        ${file}   Set Variable    random_file_${i}
        Execute    dd if=/dev/urandom of=${file} bs=1M count=10
        Execute    ozone sh key put --type=${TYPE} --replication=${REPLICATION} ${TEST_VOLUME}/${TEST_BUCKET}/${key} ${file}
    END

Cleanup Ozone Test Environment
    [Arguments]    ${num_keys}
    Log    Cleaning up Ozone test environment...
    ${keys}=    Evaluate    ${num_keys} + 1
    FOR    ${i}    IN RANGE    1    ${keys}
        Execute And Ignore Error    ozone fs -rm -skipTrash /${TEST_VOLUME}/${TEST_BUCKET}/key${i}
    END
    Execute And Ignore Error    ozone sh bucket delete /${TEST_VOLUME}/${TEST_BUCKET}
    Execute And Ignore Error    ozone sh volume delete /${TEST_VOLUME}

Corrupt Chunks
    [Arguments]    ${num_keys}
    ${keys}=    Evaluate    ${num_keys} + 1
    FOR    ${i}    IN RANGE    1    ${keys}
        ${block_id}=    Execute    ozone sh key info /${TEST_VOLUME}/${TEST_BUCKET}/key${i} | jq -r '.ozoneKeyLocations[].localID'
        ${block_path}=    Execute    bash -c "find /data/hdds/hdds -name '*${block_id}.block' | head -n 1"
        Execute    bash -c "dd if=/dev/urandom of=${block_path} bs=1 count=256 seek=256 conv=notrunc"
    END

Delete Chunks
    [Arguments]    ${num_keys}
    ${keys}=    Evaluate    ${num_keys} + 1
    FOR    ${i}    IN RANGE    1    ${keys}
        ${block_id}=    Execute    ozone sh key info /${TEST_VOLUME}/${TEST_BUCKET}/key${i} | jq -r '.ozoneKeyLocations[].localID'
        ${block_path}=    Execute    bash -c "find /data/hdds/hdds -name '*${block_id}.block' | head -n 1"
        Execute    rm -r ${block_path}
    END

Run FSCheck Damaged Keys
    [Arguments]    ${num_keys}
    ${result}=    Execute    ozone sh fscheck --verbosity-level KEY
    ${lines}=    Split To Lines    ${result}
    ${damaged_keys}=    Evaluate    [line for line in ${lines} if 'Key is damaged!' in line]
    ${damaged_count}=    Get Length    ${damaged_keys}
    Should Be Equal As Integers    ${damaged_count}    ${num_keys}
    
Run FSCheck With Delete
    Log    Running fscheck with delete option...
    Execute And Ignore Error    ozone sh fscheck --delete --verbosity-level KEY
    ${result}=    Execute    ozone sh key list ${TEST_VOLUME}/${TEST_BUCKET}
    Should Not Contain    ${result}    key

Run FSCheck With Output
    Log    Running fscheck with output
    Execute And Ignore Error    ozone sh fscheck --keys --output=${OUTPUT_FILE}
    ${file_content}=    Get File    ${OUTPUT_FILE}
    Should Contain    ${file_content}    key

Run FSCheck With Verbose Key
    [Arguments]    ${num_keys}
    Log    Running fscheck with verbose option for keys
    ${result}=    Execute    ozone sh fscheck --keys --verbosity-level=KEY
    Should Contain    ${result}    Key Information:
    ${keys}=    Evaluate    ${num_keys} + 1
    FOR    ${i}    IN RANGE    1    ${keys}
        Should Contain    ${result}    ${TEST_VOLUME}/${TEST_BUCKET}/key${i}
    END

Run FSCheck With Verbose Chunk
    [Arguments]    ${num_keys}
    Log    Running fscheck with verbose option for chunks
    ${result}=    Execute    ozone sh fscheck --keys --verbosity-level=CHUNK
    ${keys}=    Evaluate    ${num_keys} + 1
    FOR    ${i}    IN RANGE    1    ${keys}
        Should Contain    ${result}    ${TEST_VOLUME}/${TEST_BUCKET}/key${i}
    END
    Should Contain    ${result}    Chunk:

Run FSCheck With Verbose Block
    Log    Running fscheck with verbose option for blocks
    ${result}=    Execute    ozone sh fscheck --keys --verbosity-level=BLOCK
    Should Contain    ${result}    Block commit sequence id:

Run FSCheck With Verbose Container
    Log    Running fscheck with verbose option for containers
    ${result}=    Execute    ozone sh fscheck --keys --verbosity-level=CONTAINER
    Should Contain    ${result}    Container

Run FSCheck With Delete Option
    Log    Running fscheck with delete option
    ${check}=    Execute    ozone sh fscheck --delete --verbosity-level=KEY
    Should Contain    ${check}    Key is damaged!
    ${result}=    Execute    ozone sh key list /${TEST_VOLUME}/${TEST_BUCKET}
    Should Contain    ${result}    [ ]