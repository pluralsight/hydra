package hydra.common

import hydra.common.util.InstantUtils.dateStringToInstant

import java.time.Instant

object Constants {
  final val DEFAULT_LOOPHOLE_CUTOFF_DATE_FOR_TESTING: Instant = dateStringToInstant("20230619")
}
