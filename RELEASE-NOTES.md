0.27.0
======

### New/updated:
- Fix duplicate host issue for aurora scheduler
- Revert "Bump react-router-dom from 5.3.0 to 6.0.2 in /ui"
- Dummy commit 02

0.26.0
======

### New/updated:
- Enable probabilistic priority queueing by adding a task assigner plugin
- Fix CI & compile error  
- [httpofferset] put the bad offers to the bottom of the list
- upgrade mesos lib from 1.8.1 to 1.9.0
- Several project dependencies have been upgraded. See CHANGELOG for more information

0.25.0
======

### New/updated:
- Revert "Disabled pauses for auto pause enabled updates" in 0.24.2
- Several project dependencies have been upgraded. See CHANGELOG for more information.

0.24.2
======

### New/updated:
- Disabled pauses for auto pause enabled updates
- Improved HttpOfferSet performance
- Pants version bumped up to 1.26.0
- update release docs
- Several project dependencies have been upgraded. See CHANGELOG for more information.

0.24.0
======

### New/updated:
- Updated to Mesos dependency to 1.8.1.
- Pants version bumped up to 1.23.0.
- Minimum Vagrant version bumped up to 2.2.9.
- Added optional OfferSet HTTP Plugin that may be used to utilize an external process to sort
  Mesos offers.
- Several project dependencies have been upgraded. See CHANGELOG for more information.

0.23.0
======

### New/updated:
- Updated to Mesos 1.7.2.
- Updated node dependencies including eslint to 7.5.0.
- Bumped up Node version to 12.18.3
- com.github.node-gradle.node plugin bumped up to 2.2.4
- Added UNSAFE prefixes to the required functions in react code.
- Updated Quartz Scheduler to 2.3.1
- Updated Gradle to version 5.6.4
- Updated Guava to 29.0-jre
- Updated SpotBugs to 4.2.0
- Updated Commons to 3.10
- Updated Guice to 4.2.3
- Improved the way Updates handle paused/resumed updates where instance updates previously
  failed. Previously, upon resuming a paused update, previously failed updates would be retried.
  This would make updates take longer than expected, specifically when using the auto-pause
  mechanism.
- Modified auto-pause mechanism to address a corner case where the status of an instance in a batch
  would not enter a terminal state before the batch was paused. This resulted in undesired behavior
  when the updated was resumed if that instance update had failed.
- Auto-pause update mechanism now makes sure all instances have entered a terminal state
  [SUCCEDED, FAILED] before pausing the update. With this change, the final pause to
  acknowledge an update had sucessfully completed has also been dropped.
- Support has been added to use a resource with the key ips from Mesos.

0.22.0 and earlier
======
- Please see the last Apache Aurora [release notes](https://github.com/apache/attic-aurora/blob/master/RELEASE-NOTES.md)
