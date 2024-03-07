#![warn(clippy::all, clippy::pedantic, clippy::nursery)]
#![allow(clippy::option_if_let_else)]
#![allow(dead_code)]
#![allow(unused_variables)]

mod catalog;
mod dag;
mod execution;
mod logical_plan;
mod parser;
