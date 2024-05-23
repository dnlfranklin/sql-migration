<?php

namespace SqlMigration\Catalog;

class Table{

    private $name;
    private $engine;
    private $charset;
    private $collation;
    private $comments;
    private $columns = [];
    private $keys = [];
    private $fks = [];
    
    public function __construct(
        string $name, 
        string $engine = 'MyISAM', 
        string $charset = 'utf8mb4', 
        string $collation = 'utf8mb4_general_ci', 
        string $comments = ''
    ){
        $this->name = $name;
        $this->engine = $engine;
        $this->charset = $charset;
        $this->collation = $collation;
        $this->comments = $comments;
    }

    public function __get($prop){
        if(property_exists($this, $prop)){
            return $this->{$prop};
        }
    }
    
    public function addColumn(
        string $name,
        string $type,
        int $length = null,
        int $precision = null,
        string $default = null,
        bool $nullable = true,
        bool $auto_increment = false,
        bool $zerofill = false,
        bool $unsigned = false
    ):self {
        $column = new Column($name, $type, $length, $precision, $default);
        $column->setNullable($nullable);
        $column->setAutoIncrement($auto_increment);
        $column->setZeroFill($zerofill);
        $column->setUnsigned($unsigned);

        $this->columns[$name] = $column;

        return $this;
    }

    public function addKey(string $name, string $columns, string $type):self {
        $this->keys[$name] = new Key($name, $columns, $type);

        return $this;
    }

    public function addUniqueKey(string $index_name, string $columns):self {
        return $this->addKey($index_name, $columns, 'UNIQUE');
    }

    public function addPrimaryKey(string $columns):self {
        return $this->addKey('PRIMARY', $columns, 'PRIMARY KEY');
    }

    public function addIndexKey(string $index_name, string $columns):self {
        return $this->addKey($index_name, $columns, 'KEY');
    } 

    public function addForeignKey(
        string $name, 
        string $fk_column, 
        string $reference_schema, 
        string $reference_table, 
        string $reference_column
    ):self {
        $this->fks[$name] = new ForeignKey($name, $fk_column, $reference_schema, $reference_table, $reference_column);

        return $this;
    }      
    
}   