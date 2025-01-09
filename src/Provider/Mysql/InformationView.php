<?php

namespace SqlMigration\Provider\Mysql;

use SqlMigration\Catalog\View;
use SqlMigration\Core\Connector;

final class InformationView{

    public function __construct(private Connector $connector){}

    public function load(string $schema_name):Array{
        $pdo = $this->connector->get();
        
        $stmt = $pdo->prepare('
            SELECT 
                * 
            FROM 
                information_schema.tables t 
            LEFT JOIN 
                information_schema.COLLATION_CHARACTER_SET_APPLICABILITY ccsa 
                    ON ccsa.collation_name = t.table_collation 
            WHERE 
                table_schema = ? and table_type = ?');
        $stmt->bindValue(1, $schema_name);  
        $stmt->bindValue(2, 'VIEW');  
        $stmt->execute();
        $data_tables = $stmt->fetchAll();
        
        $views = [];

        foreach($data_tables as $data_table){
            $stmt = $pdo->prepare('SELECT * FROM information_schema.views WHERE table_schema = ? and table_name = ?');
            $stmt->bindValue(1, $schema_name);
            $stmt->bindValue(2, $data_table['TABLE_NAME']);
            $stmt->execute();

            $data_view = $stmt->fetch();

            $view = new View($data_view['TABLE_NAME'], $data_view['VIEW_DEFINITION']);

            $stmt = $pdo->prepare('
                SELECT 
                        col.table_schema as database_name,
                        col.table_name as view_name,
                        col.ordinal_position as ordinal_position,
                        col.column_name as column_name
                FROM 
                    information_schema.columns col
                JOIN 
                    information_schema.views vie on vie.table_schema = col.table_schema
                                                    and vie.table_name = col.table_name
                WHERE 
                    col.table_schema = ? and col.table_name = ?
                ORDER BY 
                    col.ordinal_position
            ');
            $stmt->bindValue(1, $schema_name);
            $stmt->bindValue(2, $view->name);
            $stmt->execute();

            foreach($stmt->fetchAll() as $view_column){
                $view->addColumn($view_column['column_name']);
            }

            $views[] = $view;
        }

        return $views;
    }

}