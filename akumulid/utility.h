/**
 * PUBLIC HEADER
 *
 * Library configuration data.
 *
 * Copyright (c) 2013 Eugene Lazin <4lazin@gmail.com>
 *
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
 *
 */
#pragma once

#ifdef __GNUC__
#define AKU_LIKELY(x) __builtin_expect((x), 1)
#define AKU_UNLIKELY(x) __builtin_expect((x), 0)
#else
#define AKU_LIKELY(x) (x)
#define AKU_UNLIKELY(x) (x)
#endif

//! Macro to supress `variable unused` warnings for variables that is unused for a reason.
#define AKU_UNUSED(x) (void)(x)
