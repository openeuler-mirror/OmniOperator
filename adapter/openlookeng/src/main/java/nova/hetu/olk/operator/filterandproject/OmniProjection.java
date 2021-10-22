/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2021. All rights reserved.
 */

package nova.hetu.olk.operator.filterandproject;

import static nova.hetu.olk.operator.filterandproject.OmniRowExpressionUtil.expressionStringify;
import static nova.hetu.olk.tool.OperatorUtils.toVecTypes;

import io.prestosql.spi.type.Type;
import io.prestosql.sql.relational.CallExpression;
import io.prestosql.sql.relational.InputReferenceExpression;
import io.prestosql.sql.relational.RowExpression;
import io.prestosql.sql.relational.SpecialForm;
import nova.hetu.omniruntime.operator.project.OmniProjectOperatorFactory;
import nova.hetu.omniruntime.type.VecType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * The type Omni projection.
 *
 * @since 20210630
 */
public class OmniProjection {
    private final OmniProjectOperatorFactory omniProjection;

    private final int projectLength;

    // Replace all input reference columns with the rewritten value from newColMap
    private static RowExpression rewriteColumn(RowExpression re, Map<Integer, Integer> newColMap) {
        if (re instanceof CallExpression) {
            CallExpression ce = (CallExpression) re;
            List<RowExpression> args = new ArrayList<>();
            for (RowExpression arg : ce.getArguments()) {
                args.add(rewriteColumn(arg, newColMap));
            }
            return new CallExpression(ce.getSignature(), ce.getType(), args);
        } else if (re instanceof SpecialForm) {
            SpecialForm sf = (SpecialForm) re;
            List<RowExpression> args = new ArrayList<>();
            for (RowExpression arg : sf.getArguments()) {
                args.add(rewriteColumn(arg, newColMap));
            }
            return new SpecialForm(sf.getForm(), sf.getType(), args);
        } else if (re instanceof InputReferenceExpression) {
            InputReferenceExpression ire = (InputReferenceExpression) re;
            int field = newColMap.get(ire.getField());
            return new InputReferenceExpression(field, ire.getType());
        } else {
            return re;
        }
    }

    // Adds all columns referenced by the expression re to the list cols
    private static void findColumns(RowExpression re, List<Integer> cols) {
        if (re instanceof CallExpression) {
            for (RowExpression rowExpression : ((CallExpression) re).getArguments()) {
                findColumns(rowExpression, cols);
            }
        } else if (re instanceof SpecialForm) {
            for (RowExpression rowExpression : ((SpecialForm) re).getArguments()) {
                findColumns(rowExpression, cols);
            }
        } else if (re instanceof InputReferenceExpression) {
            cols.add(((InputReferenceExpression) re).getField());
        }
    }

    /**
     * Instantiates a new Omni projection.
     *
     * @param expressions the expressions
     * @param inputTypes the input types
     * @param filter the filter
     */
    public OmniProjection(List<? extends RowExpression> expressions, List<Type> inputTypes,
        Optional<RowExpression> filter) {
        this.projectLength = expressions.size();

        String[] exprs = new String[expressions.size()];
        Set<Integer> projectColsSet = new HashSet<>();
        Map<Integer, Type> neededTypes = new HashMap<>();
        // Get all columns needed by the projection expressions
        for (int i = 0; i < exprs.length; i++) {
            List<Integer> needed = new ArrayList<>();
            findColumns(expressions.get(i), needed);
            projectColsSet.addAll(needed);
        }

        // psa[i] contains the number of columns not used by the projection expression
        // with a lower index than i.
        int[] psa = new int[inputTypes.size() + 1];
        for (int i = 1; i < psa.length; i++) {
            psa[i] = psa[i - 1];
            if (!projectColsSet.contains(i - 1)) {
                psa[i]++;
            }
        }
        Map<Integer, Integer> newColMap = new HashMap<>();
        for (int pc : projectColsSet) {
            newColMap.put(pc, pc - psa[pc]);
        }
        for (int i = 0; i < exprs.length; i++) {
            RowExpression fixed = rewriteColumn(expressions.get(i), newColMap);
            exprs[i] = expressionStringify(fixed);
            List<Integer> channels = new ArrayList<>();
            findColumns(expressions.get(i), channels);
            for (int v : channels) {
                neededTypes.put(v, inputTypes.get(v));
            }
        }

        // get the types and sort by column index
        List<Type> sortedTypes = neededTypes.keySet()
            .stream()
            .sorted()
            .map(neededTypes::get)
            .collect(Collectors.toList());

        VecType[] convertedTypes = toVecTypes(sortedTypes);
        this.omniProjection = new OmniProjectOperatorFactory(exprs, convertedTypes);
    }

    /**
     * Gets factory.
     *
     * @return the factory
     */
    public OmniProjectOperatorFactory getFactory() {
        return this.omniProjection;
    }

    /**
     * Is empty boolean.
     *
     * @return the boolean
     */
    public boolean isEmpty() {
        return this.projectLength == 0;
    }

    /**
     * Size int.
     *
     * @return the int
     */
    public int size() {
        return this.projectLength;
    }
}
