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
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgnitionEx;
import org.apache.ignite.internal.client.marshaller.GridClientMarshaller;
import org.apache.ignite.internal.util.typedef.F;
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
        final String name = setGridName(gridName);

        try {
            marshaller.marshal(obj, out);
        } finally {
            restoreGridName(name);
        }
    }

    /**
     *
     * @param marshaller marshaller.
     * @param obj object.
     * @param igniteCfg ignite config.
     * @return serialized.
     * @throws IgniteCheckedException
     */
    public static byte[] marshal(final Marshaller marshaller, @Nullable Object obj,
        final IgniteConfiguration igniteCfg) throws IgniteCheckedException {
        return marshal(marshaller, obj, igniteCfg.getGridName());
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
        final String name = setGridName(gridName);

        try {
            return marshaller.marshal(obj);
        } finally {
            restoreGridName(name);
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
        final String name = setGridName(gridName);

        try {
            return marshaller.unmarshal(in, clsLdr);
        } finally {
            restoreGridName(name);
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
        final String name = setGridName(gridName);

        try {
            return marshaller.unmarshal(arr, clsLdr);
        } finally {
            restoreGridName(name);
        }
    }

    /**
     *
     * @param marshaller marshaller.
     * @param arr byte array.
     * @param clsLdr class loader.
     * @param igniteCfg ignite config.
     * @param <T> target type
     * @return deserialized object.
     * @throws IgniteCheckedException
     */
    public static <T> T unmarshal(final Marshaller marshaller, byte[] arr, @Nullable ClassLoader clsLdr,
        final IgniteConfiguration igniteCfg) throws IgniteCheckedException {
        return unmarshal(marshaller, arr, clsLdr, igniteCfg.getGridName());
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
        final String name = setGridName(gridName);

        try {
            return marshaller.unmarshal(marshaller.marshal(obj), clsLdr);
        } finally {
            restoreGridName(name);
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
        final String name = setGridName(gridName);

        try {
            return gridMarshaller.marshal(obj, off);
        } finally {
            restoreGridName(name);
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
        final String name = setGridName(gridName);

        try {
            return gridMarshaller.unmarshal(bytes);
        } finally {
            restoreGridName(name);
        }
    }

    /**
     * @param name Grid name.
     * @return Old grid name.
     */
    private static String setGridName(final String name) {
        final String gridName = IgnitionEx.getGridNameThreadLocal();

        if (!F.eq(name, gridName))
            IgnitionEx.setGridNameThreadLocal(name);

        return gridName;
    }

    /**
     * @param name Grid name.
     */
    private static void restoreGridName(final String name) {
        IgnitionEx.setGridNameThreadLocal(name);
    }

    /**
     *
     * @param kernalCtx kernal context.
     * @return ignite config or null.
     */
    private static IgniteConfiguration getConfig(final @Nullable GridKernalContext kernalCtx) {
        return kernalCtx == null ? null : kernalCtx.config();
    }
}
