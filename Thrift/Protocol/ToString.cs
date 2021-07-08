// Licensed to the Apache Software Foundation(ASF) under one
// or more contributor license agreements.See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
// 
//     http://www.apache.org/licenses/LICENSE-2.0
// 
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

using System;
using System.Collections;
using System.Collections.Generic;
using System.Text;
using Thrift.Protocol;

namespace Thrift.Protocol
{
    public static class ToStringExtensions
    {
        public static void ToString(this string str, StringBuilder sb, bool first = true)
        {
            if (!first)
                sb.Append(", ");

            sb.Append('"');
            sb.Append(str);
            sb.Append('"');
        }

        public static void ToString(this IDictionary dic, StringBuilder sb, bool first = true)
        {
            if (!first)
                sb.Append(", ");

            bool first_child = true;

            sb.Append("{ ");

            foreach (DictionaryEntry pair in dic)
            {
                if (first_child)
                    first_child = false;
                else
                    sb.Append(",");

                sb.Append("{ ");
                pair.Key.ToString(sb);
                sb.Append(", ");
                pair.Value.ToString(sb);
                sb.Append("}");
            }

            sb.Append("}");
        }

        public static void ToString(this IEnumerable value, StringBuilder sb, bool first = true)
        {
            if (!first)
                sb.Append(", ");

            bool first_child = true;

            sb.Append("{ ");

            foreach (var elm in value)
            {
                elm.ToString(sb, first_child);
                first_child = false;
            }

            sb.Append("}");
        }

        public static void ToString(this TBase value, StringBuilder sb, bool first = true)
        {
            if (!first)
                sb.Append(", ");

            sb.Append(value);
        }

        public static void ToString(this object self, StringBuilder sb, bool first = true)
        {
            if (!first)
                sb.Append(", ");

            sb.Append(self != null ? self.ToString() : "<null>");
        }
    }
}
