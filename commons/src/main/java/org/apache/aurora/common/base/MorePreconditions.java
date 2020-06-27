/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.aurora.common.base;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;

import org.apache.commons.lang3.StringUtils;

/**
 * A utility helpful in concisely checking preconditions on arguments.  This utility is a complement
 * to {@link com.google.common.base.Preconditions}.
 *
 * @author John Sirois
 */
public final class MorePreconditions {

  private static final String ARG_NOT_BLANK_MSG = "Argument cannot be blank";

  private MorePreconditions() {
    // utility
  }

  /**
   * Checks that a string is both non-null and non-empty.
   *
   * @see #checkNotBlank(String, String, Object...)
   */
  public static String checkNotBlank(String argument) {
    return checkNotBlank(argument, ARG_NOT_BLANK_MSG);
  }

  /**
   * Checks that a string is both non-null and non-empty.
   *
   * @param argument the argument to validate
   * @param message the message template for validation exception messages where %s serves as the
   *     sole argument placeholder
   * @param args any arguments needed by the message template
   * @return the argument if it is valid
   * @throws NullPointerException if the argument is null
   * @throws IllegalArgumentException if the argument is the empty string or a pure whitespace
   *    string
   */
  public static String checkNotBlank(String argument, String message, Object... args) {
    Preconditions.checkNotNull(argument, message, args);
    Preconditions.checkArgument(!StringUtils.isBlank(argument), message, args);
    return argument;
  }

  /**
   * Checks that an Iterable is both non-null and non-empty.  This method does not check individual
   * elements in the Iterable.
   *
   * @see #checkNotBlank(Iterable, String, Object...)
   */
  public static <S, T extends Iterable<S>> T checkNotBlank(T argument) {
    return checkNotBlank(argument, ARG_NOT_BLANK_MSG);
  }

  /**
   * Checks that an Iterable is both non-null and non-empty.  This method does not check individual
   * elements in the Iterable, it just checks that the Iterable has at least one element.
   *
   * @param argument the argument to validate
   * @param message the message template for validation exception messages where %s serves as the
   *     sole argument placeholder
   * @param args any arguments needed by the message template
   * @return the argument if it is valid
   * @throws NullPointerException if the argument is null
   * @throws IllegalArgumentException if the argument has no iterable elements
   */
  public static <S, T extends Iterable<S>> T checkNotBlank(T argument, String message,
      Object... args) {
    Preconditions.checkNotNull(argument, message, args);
    Preconditions.checkArgument(!Iterables.isEmpty(argument), message, args);
    return argument;
  }
}
