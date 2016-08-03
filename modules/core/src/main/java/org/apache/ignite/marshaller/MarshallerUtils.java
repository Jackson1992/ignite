/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.marshaller;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.LocalGridName;
import org.apache.ignite.internal.IgnitionEx;
import org.apache.ignite.internal.client.marshaller.GridClientMarshaller;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;

/**
 * Util class that sets and discards thread local
 * ignite configuration in {@link IgnitionEx} class.
 */
public final class MarshallerUtils {
    /**
     *
     */
    private MarshallerUtils() {
    }

    /**
     *
     * @param marshaller marshaller.
     * @param obj object.
     * @param out output stream.
     * @param gridName Grid name.
     * @throws IgniteCheckedException
     */
    public static void marshal(final Marshaller marshaller, final @Nullable Object obj,
        final OutputStream out, final String gridName) throws IgniteCheckedException {
        final LocalGridName gridNameTl = gridName();

        final String gridNameStr = gridNameTl.getGridName();
        final boolean init = gridNameTl.isSet();

        try {
            gridNameTl.setGridName(true, gridName);

            marshaller.marshal(obj, out);
        } finally {
            gridNameTl.setGridName(init, gridNameStr);
        }
    }

    /**
     *
     * @param marshaller marshaller.
     * @param obj object.
     * @param gridName Grid name.
     * @return serialized.
     * @throws IgniteCheckedException
     */
    public static byte[] marshal(final Marshaller marshaller, @Nullable Object obj,
        final String gridName) throws IgniteCheckedException {
        final LocalGridName gridNameTl = gridName();

        final String gridNameStr = gridNameTl.getGridName();
        final boolean init = gridNameTl.isSet();

        try {
            gridNameTl.setGridName(true, gridName);

            return marshaller.marshal(obj);
        } finally {
            gridNameTl.setGridName(init, gridNameStr);
        }
    }

    /**
     *
     * @param marshaller marshaller.
     * @param in input stream.
     * @param clsLdr class loader.
     * @param gridName Grid name.
     * @param <T> target type.
     * @return deserialized object.
     * @throws IgniteCheckedException
     */
    public static <T> T unmarshal(final Marshaller marshaller, InputStream in, @Nullable ClassLoader clsLdr,
        final String gridName) throws IgniteCheckedException {
        final LocalGridName gridNameTl = gridName();

        final String gridNameStr = gridNameTl.getGridName();
        final boolean init = gridNameTl.isSet();

        try {
            gridNameTl.setGridName(true, gridName);

            return marshaller.unmarshal(in, clsLdr);
        } finally {
            gridNameTl.setGridName(init, gridNameStr);
        }
    }

    /**
     *
     * @param marshaller marshaller.
     * @param arr byte array.
     * @param clsLdr class loader.
     * @param gridName Grid name.
     * @param <T> target type
     * @return deserialized object.
     * @throws IgniteCheckedException
     */
    public static <T> T unmarshal(final Marshaller marshaller, byte[] arr, @Nullable ClassLoader clsLdr,
        final String gridName) throws IgniteCheckedException {
        final LocalGridName gridNameTl = gridName();

        final String gridNameStr = gridNameTl.getGridName();
        final boolean init = gridNameTl.isSet();

        try {
            gridNameTl.setGridName(true, gridName);

            return marshaller.unmarshal(arr, clsLdr);
        } finally {
            gridNameTl.setGridName(init, gridNameStr);
        }
    }

    /**
     *
     * @param marshaller marshaller.
     * @param obj object
     * @param clsLdr class loader.
     * @param gridName Grid name.
     * @param <T> target type.
     * @return deserialized value.
     * @throws IgniteCheckedException
     */
    public static <T> T clone(final Marshaller marshaller, T obj, @Nullable ClassLoader clsLdr,
        final String gridName) throws IgniteCheckedException {
        final LocalGridName gridNameTl = gridName();

        final String gridNameStr = gridNameTl.getGridName();
        final boolean init = gridNameTl.isSet();

        try {
            gridNameTl.setGridName(true, gridName);

            return marshaller.unmarshal(marshaller.marshal(obj), clsLdr);
        } finally {
            gridNameTl.setGridName(init, gridNameStr);
        }
    }

    /**
     *
     * @param gridMarshaller grid marshaller.
     * @param obj object.
     * @param off offset.
     * @param gridName ignite config.
     * @return serialized data.
     * @throws IOException
     */
    public static ByteBuffer marshal(GridClientMarshaller gridMarshaller, Object obj, int off,
        String gridName) throws IOException {
        final LocalGridName gridNameTl = gridName();

        final String gridNameStr = gridNameTl.getGridName();
        final boolean init = gridNameTl.isSet();

        try {
            gridNameTl.setGridName(true, gridName);

            return gridMarshaller.marshal(obj, off);
        } finally {
            gridNameTl.setGridName(init, gridNameStr);
        }
    }

    /**
     *
     * @param gridMarshaller grid marshaller.
     * @param bytes byte array.
     * @param gridName ignite config.
     * @param <T> target type.
     * @return deserialized value.
     * @throws IOException
     */
    public static <T> T unmarshal(GridClientMarshaller gridMarshaller, byte[] bytes,
        String gridName) throws IOException {
        final LocalGridName gridNameTl = gridName();

        final String gridNameStr = gridNameTl.getGridName();
        final boolean init = gridNameTl.isSet();

        try {
            gridNameTl.setGridName(true, gridName);

            return gridMarshaller.unmarshal(bytes);
        } finally {
            gridNameTl.setGridName(init, gridNameStr);
        }
    }

    /**
     * @return Grid name thread local.
     */
    private static LocalGridName gridName() {
        return IgnitionEx.gridNameThreadLocal();
    }
}
