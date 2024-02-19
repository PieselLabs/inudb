use sqlparser::ast;
use crate::catalog::{Catalog, DummyCatalog};
use crate::logical_plan::{ColumnExpr, Dag, DagBuilder, Expr, LogicalPlan};

use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use crate::dag::NodeId;

struct SQLParser {}

impl SQLParser {
    pub fn new(sql_query: &str, catalog: &DummyCatalog) -> Dag<LogicalPlan> {
        let dialect = GenericDialect {};
        let statements = Parser::parse_sql(&dialect, sql_query).unwrap();

        assert_eq!(statements.len(), 1);
        let statement = &statements[0];
        let mut dag= Dag::new();
        let mut dag_builder = DagBuilder::new(&mut dag);

        let mut query: NodeId = 0;

        match statement {
            ast::Statement::Query(q) => query = Self::parse_query(q, &mut dag_builder, catalog),
            _ => unimplemented!(),
        }
        dag
    }

    fn parse_query(query: &ast::Query, dag_builder: &mut DagBuilder, catalog: &DummyCatalog) -> NodeId {
        match *query.body {
            ast::SetExpr::Select(ref select) => Self::parse_select(select, dag_builder, catalog),
            _ => unimplemented!(),
        }
    }

    fn parse_select(select: &ast::Select, dag_builder: &mut DagBuilder, catalog: &DummyCatalog) -> NodeId {
        assert_eq!(select.from.len(), 1);
        let from_id = Self::parse_from(&select.from[0], dag_builder, catalog);

        let projection_id = Self::parse_projection(&select.projection, dag_builder, from_id);
        
        // if let Some(filter) = &select.selection {
        //     Self::parse_where(filter, dag_builder);
        // }
        
        projection_id
    }

    fn parse_from(table: &ast::TableWithJoins, dag_builder: &mut DagBuilder, catalog: &DummyCatalog) -> NodeId {
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

    fn parse_projection(projection: &[ast::SelectItem], dag_builder: &mut DagBuilder, from_id: NodeId) -> NodeId {
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
        
        let mut vec_expr : Vec<Expr> = vec![];
        for column_name in columns.iter() {
            vec_expr.push(Expr::Column(ColumnExpr {name: column_name.to_string() }));
        }

        dag_builder.create_project(vec_expr, from_id)
    }

    // fn parse_where(expr: &ast::Expr, dag_builder: &mut DagBuilder) -> NodeId {
    //     match expr { ast::Expr::BinaryOp{left, op, right} => println!("{}", op),
    //     //     _ => unimplemented!()
    //     // }
    //     println!("Where")
    // }

}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use sqlparser::dialect::GenericDialect;
    use crate::catalog::DummyCatalog;
    use crate::dag::Dag;
    use crate::logical_plan::{ColumnExpr, DagBuilder, Expr, LogicalPlan};
    use crate::parser::sql_parser::SQLParser;

    #[test]
    fn test_sql_parser() {
        let dialect = GenericDialect {};

        let sql_query = "SELECT a, b FROM table_1";
        // let sql_query = "SELECT a, b FROM table_1 WHERE a > 50 AND b < 100";

        let mut catalog = DummyCatalog::new();

        let table_1_field_a =  arrow::datatypes::Field::new("a", arrow::datatypes::DataType::Int32, false);
        let table_1_field_b = arrow::datatypes::Field::new("b", arrow::datatypes::DataType::Int32, false);
        let table_1_schema = Arc::new(arrow::datatypes::Schema::new(vec![table_1_field_a, table_1_field_b]));
        catalog.add_table("table_1", table_1_schema.clone());

        let logical_plan_actual = SQLParser::new(sql_query, &mut catalog);

        let mut logical_plan_excepted : Dag<LogicalPlan> = Dag::new();

        let mut dag_builder = DagBuilder::new(&mut logical_plan_excepted);

        let project_expr = vec![Expr::Column(ColumnExpr{name: "a".to_string()}), Expr::Column(ColumnExpr{name: "b".to_string()})];
        let scan_id = dag_builder.create_scan("table_1".to_string(), table_1_schema.clone());
        dag_builder.create_project(project_expr, scan_id);

        assert_eq!(logical_plan_actual.get_node(0), logical_plan_excepted.get_node(0));
        assert_eq!(logical_plan_actual.get_node(1), logical_plan_excepted.get_node(1));
    }
}
