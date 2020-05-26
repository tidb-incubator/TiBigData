/*
 * Copyright 2020 Zhihu.
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
 */
package com.zhihu.prestodb.tidb;

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.FixedSplitSource;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.zhihu.presto.tidb.SplitInternal;
import com.zhihu.presto.tidb.SplitManagerInternal;

import javax.inject.Inject;

import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;

public final class TiDBSplitManager
        extends Wrapper<SplitManagerInternal>
        implements ConnectorSplitManager
{

    @Inject
    public TiDBSplitManager(TiDBSession session)
    {
        super(new SplitManagerInternal(session.getInternal()));
    }

    @Override
    public ConnectorSplitSource getSplits(
            ConnectorTransactionHandle handle,
            ConnectorSession session,
            ConnectorTableLayoutHandle layout,
            SplitSchedulingContext splitSchedulingContext)
    {
        TiDBTableLayoutHandle layoutHandle = (TiDBTableLayoutHandle) layout;
        TiDBTableHandle tableHandle = layoutHandle.getTable();
        List<SplitInternal> splits = getInternal().getSplits(tableHandle.getInternal());
        return new FixedSplitSource(splits.stream().map(s -> new TiDBSplit(s, layoutHandle.getAdditionalPredicate())).collect(toImmutableList()));
    }
}
