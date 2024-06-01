<?php

namespace SqlMigration\Provider\Mysql;

use SqlMigration\Catalog\Table;
use SqlMigration\Core\Connector;

final class InformationTable{

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
                    ON CCSA.collation_name = t.table_collation 
            WHERE 
                table_schema = ? and table_type = ?');
        $stmt->bindValue(1, $schema_name);  
        $stmt->bindValue(2, 'BASE TABLE');  
        $stmt->execute();
        $data_tables = $stmt->fetchAll();
        
        $tables = [];

        foreach($data_tables as $data_table){
            $table = new Table(
                $data_table['TABLE_NAME'], 
                $data_table['ENGINE'], 
                $data_table['CHARACTER_SET_NAME'], 
                $data_table['TABLE_COLLATION'], 
                $data_table['TABLE_COMMENT']
            );
            
            $stmt = $pdo->prepare('
                SELECT 
                    table_schema AS table_schema,
                    table_name AS table_name,
                    ordinal_position AS ordinal_position,
                    column_name AS column_name,
                    data_type AS data_type,
                    column_type AS column_type,
                    case 
                        when numeric_precision is not null then numeric_precision 
                        else character_maximum_length 
                    end as max_length,
                    case 
                        when datetime_precision is not null then datetime_precision 
                        when numeric_scale is not null then numeric_scale
                        else null
                    end as data_precision,
                    is_nullable AS is_nullable,
                    column_default AS column_default,
                    extra AS extra
                FROM 
                    information_schema.columns
                WHERE 
                    table_schema = ? and table_name = ?
                ORDER BY 
                    ordinal_position
            ');
            $stmt->bindValue(1, $schema_name);
            $stmt->bindValue(2, $table->name);
            $stmt->execute();

            foreach($stmt->fetchAll() as $column){
                $column_type = explode(' ', $column['column_type'])[0];
                $column_type = str_replace([$column['data_type'], '(', ')'], '', $column_type);

                if($column_type == ''){
                    $column_length = null;
                    $column_precision = null;
                }else{
                    $length_precision = explode(',', $column_type);
                    $column_length = empty($length_precision[0]) ? null : (int) $length_precision[0];
                    $column_precision = empty($length_precision[1]) ? null : (int) $length_precision[1];
                }
                
                $table->addColumn(
                    $column['column_name'],
                    $column['data_type'],
                    $column_length,
                    $column_precision,
                    $column['column_default'],
                    $column['is_nullable'] == 'YES' ? true : false,
                    stristr($column['extra'], 'auto_increment') !== FALSE ? true : false,
                    stristr($column['column_type'], 'zerofill') !== FALSE ? true : false,
                    stristr($column['column_type'], 'unsigned') !== FALSE ? true : false
                );
            }

            $stmt = $pdo->prepare("
                SELECT 
                    stat.table_schema AS table_schema,
                    stat.table_name AS table_name,
                    stat.index_name AS index_name,
                    group_concat(stat.column_name order by stat.seq_in_index separator ',') as columns,
                    case 
                        when tco.constraint_type IS NULL then 'KEY' 
                        else tco.constraint_type 
                    end as constraint_type
                FROM 
                    information_schema.statistics stat
                LEFT JOIN 
                    information_schema.table_constraints tco on 
                        tco.table_name = stat.table_name and 
                        tco.table_schema = stat.table_schema and 
                        tco.constraint_name = stat.index_name and 
                        tco.constraint_type != 'FOREIGN KEY'
                WHERE 
                    stat.table_schema = ? and stat.table_name = ?
                GROUP BY
                    stat.table_schema,
                    stat.table_name,
                    stat.index_name,
                    tco.constraint_type
            ");
            $stmt->bindValue(1, $schema_name);
            $stmt->bindValue(2, $table->name);
            $stmt->execute();

            foreach($stmt->fetchAll() as $key){
                $table->addKey($key['index_name'], $key['columns'], $key['constraint_type']);
            }

            $stmt = $pdo->prepare("
                SELECT   
                        fks.constraint_schema as foreign_schema,
                        fks.table_name as foreign_table,
                        kcu.column_name as foreign_column_name,
                        fks.constraint_name as foreign_constraint_name,
                        fks.unique_constraint_schema as reference_schema,
                        fks.referenced_table_name as reference_table,
                        kcu.referenced_column_name as reference_column_name
                FROM information_schema.referential_constraints fks
                JOIN information_schema.key_column_usage kcu
                    on fks.constraint_schema = kcu.table_schema
                    and fks.table_name = kcu.table_name
                    and fks.constraint_name = kcu.constraint_name
                    and kcu.POSITION_IN_UNIQUE_CONSTRAINT IS NOT NULL
                WHERE 
                    fks.constraint_schema = ? AND fks.table_name = ?
                ORDER BY 
                    kcu.ordinal_position
            ");
            $stmt->bindValue(1, $schema_name);
            $stmt->bindValue(2, $table->name);
            $stmt->execute();

            foreach($stmt->fetchAll() as $fk){
                $table->addForeignKey(
                    $fk['foreign_constraint_name'], 
                    $fk['foreign_column_name'], 
                    $fk['reference_schema'],
                    $fk['reference_table'],
                    $fk['reference_column_name'],
                );
            }
            
            $tables[] = $table;
        }

        return $tables;
    }

}