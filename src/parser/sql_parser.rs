use crate::catalog::{Catalog, DummyCatalog};
use sqlparser::ast;

use crate::dag::NodeId;
use crate::logical_plan::dag_builder::DagBuilder;
use crate::logical_plan::expr::{Expr, Ident, VisitExpression};
use crate::logical_plan::{Dag, LogicalPlan};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

pub fn parse_sql_query(sql_query: &str, catalog: &DummyCatalog) -> Dag<LogicalPlan> {
    let dialect = GenericDialect {};
    let statements = Parser::parse_sql(&dialect, sql_query).unwrap();

    // println!("{:?}", {statements.clone()});

    assert_eq!(statements.len(), 1);
    let statement = &statements[0];
    let mut dag = Dag::new();
    let mut dag_builder = DagBuilder::new(&mut dag);

    match statement {
        ast::Statement::Query(q) => {
            parse_query(q, &mut dag_builder, catalog);
        }
        _ => unimplemented!(),
    }
    dag
}

fn parse_query(query: &ast::Query, dag_builder: &mut DagBuilder, catalog: &DummyCatalog) -> NodeId {
    match *query.body {
        ast::SetExpr::Select(ref select) => parse_select(select, dag_builder, catalog),
        _ => unimplemented!(),
    }
}

fn parse_select(
    select: &ast::Select,
    dag_builder: &mut DagBuilder,
    catalog: &DummyCatalog,
) -> NodeId {
    assert_eq!(select.from.len(), 1);
    let from_id = parse_from(&select.from[0], dag_builder, catalog);

    let mut result = from_id;

    if let Some(filter) = &select.selection {
        result = parse_where(filter, dag_builder, result);
    }

    parse_projection(&select.projection, dag_builder, result)
}

fn parse_from(
    table: &ast::TableWithJoins,
    dag_builder: &mut DagBuilder,
    catalog: &DummyCatalog,
) -> NodeId {
    assert!(table.joins.is_empty());
    match &table.relation {
        ast::TableFactor::Table { name, .. } => {
            let table_name = name.to_string();
            let schema = catalog.get_schema(table_name.clone().leak()).unwrap();
            dag_builder.create_scan(table_name, schema)
        }
        _ => unimplemented!(),
    }
}

fn parse_projection(
    projection: &[ast::SelectItem],
    dag_builder: &mut DagBuilder,
    from_id: NodeId,
) -> NodeId {
    let columns: Vec<_> = projection
        .iter()
        .map(|item| match item {
            ast::SelectItem::UnnamedExpr(expr) => match expr {
                ast::Expr::Identifier(ident) => ident.value.clone(),
                _ => unimplemented!(),
            },
            _ => unimplemented!(),
        })
        .collect();

    let mut vec_expr: Vec<Expr> = vec![];
    for column_name in &columns {
        vec_expr.push(Expr::Ident(Ident {
            name: column_name.to_string(),
        }));
    }

    dag_builder.create_project(vec_expr, from_id)
}

fn parse_where(expr: &ast::Expr, dag_builder: &mut DagBuilder, input: NodeId) -> NodeId {
    let expression = VisitExpression::visit(expr);
    dag_builder.create_filter(Box::new(expression), input)
}

#[cfg(test)]
mod tests {
    use crate::catalog::DummyCatalog;
    use crate::dag::Dag;

    use crate::logical_plan::dag_builder::DagBuilder;
    use crate::logical_plan::expr::{Binary, BinaryOp, Expr, Ident, IntegerLiteral};
    use crate::logical_plan::LogicalPlan;
    use crate::parser::sql_parser::parse_sql_query;
    use sqlparser::dialect::GenericDialect;
    use std::sync::Arc;

    #[test]
    fn test_sql_parser() {
        let dialect = GenericDialect {};

        let sql_query = "SELECT a, b FROM table_1";

        let mut catalog = DummyCatalog::new();

        let table_1_field_a =
            arrow::datatypes::Field::new("a", arrow::datatypes::DataType::Int32, false);
        let table_1_field_b =
            arrow::datatypes::Field::new("b", arrow::datatypes::DataType::Int32, false);
        let table_1_schema = Arc::new(arrow::datatypes::Schema::new(vec![
            table_1_field_a,
            table_1_field_b,
        ]));
        catalog.add_table("table_1", table_1_schema.clone());

        let logical_plan_actual = parse_sql_query(sql_query, &mut catalog);

        let mut logical_plan_excepted: Dag<LogicalPlan> = Dag::new();

        let mut dag_builder = DagBuilder::new(&mut logical_plan_excepted);

        let project_expr = vec![
            Expr::Ident(Ident {
                name: "a".to_string(),
            }),
            Expr::Ident(Ident {
                name: "b".to_string(),
            }),
        ];
        let scan_id = dag_builder.create_scan("table_1".to_string(), table_1_schema.clone());
        dag_builder.create_project(project_expr, scan_id);

        assert_eq!(
            logical_plan_actual.get_node(0),
            logical_plan_excepted.get_node(0)
        );
        assert_eq!(
            logical_plan_actual.get_node(1),
            logical_plan_excepted.get_node(1)
        );
    }

    #[test]
    fn test_sql_parser_with_filter() {
        let dialect = GenericDialect {};

        let sql_query = "SELECT a, b FROM table_1 WHERE a > 50 AND b < 100";

        let mut catalog = DummyCatalog::new();

        let table_1_field_a =
            arrow::datatypes::Field::new("a", arrow::datatypes::DataType::Int32, false);
        let table_1_field_b =
            arrow::datatypes::Field::new("b", arrow::datatypes::DataType::Int32, false);
        let table_1_schema = Arc::new(arrow::datatypes::Schema::new(vec![
            table_1_field_a,
            table_1_field_b,
        ]));
        catalog.add_table("table_1", table_1_schema.clone());

        let logical_plan_actual = parse_sql_query(sql_query, &mut catalog);

        let mut logical_plan_excepted: Dag<LogicalPlan> = Dag::new();

        let mut dag_builder = DagBuilder::new(&mut logical_plan_excepted);

        let project_expr = vec![
            Expr::Ident(Ident {
                name: "a".to_string(),
            }),
            Expr::Ident(Ident {
                name: "b".to_string(),
            }),
        ];
        let filter_expr = Box::from(Expr::Binary(Binary {
            lhs: Box::from(Expr::Binary(Binary {
                lhs: Box::from(Expr::Ident(Ident {
                    name: "a".to_string(),
                })),
                op: BinaryOp::Gt,
                rhs: Box::from(Expr::IntegerLiteral(IntegerLiteral { value: 50 })),
            })),
            op: BinaryOp::And,
            rhs: Box::from(Expr::Binary(Binary {
                lhs: Box::from(Expr::Ident(Ident {
                    name: "b".to_string(),
                })),
                op: BinaryOp::Lt,
                rhs: Box::from(Expr::IntegerLiteral(IntegerLiteral { value: 100 })),
            })),
        }));

        let scan = dag_builder.create_scan("table_1".to_string(), table_1_schema.clone());
        let filter = dag_builder.create_filter(filter_expr, scan);
        let project = dag_builder.create_project(project_expr, filter);

        assert_eq!(
            logical_plan_actual.get_node(0),
            logical_plan_excepted.get_node(0)
        );
        assert_eq!(
            logical_plan_actual.get_node(1),
            logical_plan_excepted.get_node(1)
        );
        assert_eq!(
            logical_plan_actual.get_node(2),
            logical_plan_excepted.get_node(2)
        );
    }
}
