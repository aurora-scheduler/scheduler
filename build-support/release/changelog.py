#!/usr/bin/python3
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""
Helper script to parse json payload from Github containing information about the project.

Using the milestone title as a key, it looks for information to be used for the changelog.
"""

import sys
import json

version = "undefined"
if len(sys.argv) > 1:
    version = sys.argv[1]

issues = []
prs = []

data = json.load(sys.stdin)

milestones = data["data"]["repository"]["milestones"]["nodes"]
milestone = None

for m in milestones:
    if m["title"] == version:
        milestone = m

if milestone is None:
    sys.exit(f'no open milestone found on github matching provied version {version}')

print(f"Aurora Scheduler {version}")
print("-" * 80)

if len(milestone["pullRequests"]["nodes"]) > 0:
    print("## Pull Requests")
    for pr in milestone["pullRequests"]["nodes"]:
        print(f'* [#{pr["number"]}]({pr["permalink"]}) - {pr["title"]}')

    print("")

if len(milestone["issues"]["nodes"]) > 0:
    print("## Issues")
    for issue in milestone["issues"]["nodes"]:
        print(f'* [#{issue["number"]}]({issue["url"]}) - {issue["title"]}')

    print("")