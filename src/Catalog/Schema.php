<?php

namespace SqlMigration\Catalog;

class Schema{

    private $name;
    private $charset;
    private $collation;
    private $drop;
    private $tables = [];
    private $views = [];
    private $data = [];

    public function __construct(string $name, string $charset = null, string $collation = null){
        $this->name = $name;
        $this->charset = $charset;
        $this->collation = $collation;
        $this->drop = new Drop;
    }

    public function __get($prop){
        if(property_exists($this, $prop)){
            return $this->{$prop};
        }
    }

    public function dropTable(string $table_name):self {
        $this->drop->table($table_name);        

        return $this;
    }

    public function dropColumn(string $table_name, string $column_name):self {
        $this->drop->column($table_name, $column_name);        

        return $this;
    }

    public function dropKey(string $table_name, string $key_name):self {
        $this->drop->key($table_name, $key_name);

        return $this;
    }

    public function dropForeignKey(string $table_name, string $foreign_key_name):self {
        $this->drop->fk($table_name, $foreign_key_name);

        return $this;
    }

    public function addTable(Table $table):self {
        $this->tables[$table->name] = $table;

        return $this;
    }

    public function addView(View $view):self {
        $this->views[$view->name] = $view;

        return $this;
    }

    public function newData(Data $data):self {
        $this->data[] = $data;

        return $this;
    }
    
}   