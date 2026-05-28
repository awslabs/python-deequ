# Copyright 2024 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

"""Test fixtures module.

Contains test dataset definitions ported from the original Deequ Scala
implementation for comprehensive edge case coverage.
"""

from .datasets import (
    create_df_full,
    create_df_missing,
    create_df_numeric,
    create_df_unique,
    create_df_distinct,
    create_df_string_lengths,
    create_df_empty,
    create_df_single,
    create_df_all_null,
    create_df_escape,
    create_df_correlation,
    create_df_entropy,
    create_df_where,
    EXPECTED_VALUES,
)
