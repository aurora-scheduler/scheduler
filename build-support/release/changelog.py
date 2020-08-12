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

for issue in data:
    if ("milestone" in issue and
            issue["milestone"] is not None and
            issue["milestone"]["title"] == version):
        if "pull_request" in issue:
            prs.append(f'#{issue["number"]} - {issue["title"]}')
        else:
            issues.append(f'#{issue["number"]} - {issue["title"]}')

print(f"Aurora Scheduler {version}")
print("-" * 80)
if len(issues) > 0:
    print("## Issues")
    for issue in issues:
        print(f"  * {issue}")
    print("")

if len(prs) > 0:
    print("## Pull Requests")
    for pr in prs:
        print(f"  * {pr}")
    print("")