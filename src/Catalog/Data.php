<?php

namespace SqlMigration\Catalog;

class Data{

    private $table_name;
    private $identifier_columns = [];
    private $columns_name = [];
    private $columns_data = [];

    public function __construct(string $table_name){
        $this->table_name  = $table_name;
    }

    public function __get($prop){
        if($prop == 'identifier_columns'){
            return $this->getIdentifierColumns();
        }

        if(property_exists($this, $prop)){
            return $this->{$prop};
        }
    }

    public function setIdentifierColumns(string ...$columns){
        $this->identifier_columns = $columns;

        if(!empty($this->columns_name)){
            foreach($columns as $column){
                if(!in_array($column, $this->columns_name)){
                    throw new \Exception('Identifier column does not match a data column');
                }
            }
        }

        return $this;
    }

    public function setColumns(string ...$columns):self {
        $this->columns_name = $columns;

        if(!empty($this->identifier_columns)){
            foreach($columns as $column){
                if(!in_array($column, $this->identifier_columns)){
                    throw new \Exception('Identifier column does not match a data column');
                }
            }
        }

        return $this;
    }

    public function add(mixed ...$columns):self {
        if(count($columns) != count($this->columns_name)){
            throw new \Exception('Incompatible number of columns');
        }
        
        $this->columns_data[] = $columns;

        return $this;
    }

    public function getIdentifierColumns(){
        return empty($this->identifier_columns) ? $this->columns_name : $this->identifier_columns;
    }

    public function getHashes(string ...$columns):Array {
        $identifier_hash = $this->getIdentifierColumns();        

        $hashes = [];

        foreach($this->columns_data as $column_data){
            $concat = '';
            $content = [];

            foreach($identifier_hash as $identifier){
                $pos = array_search($identifier, $this->columns_name);
                $concat.= $column_data[$pos];
            }
            
            $hash = md5($concat);

            foreach($columns as $column){
                $pos = array_search($column, $this->columns_name);
                $content[$column] = $column_data[$pos];     
            }

            $hashes[$hash] = serialize($content);
        }

        return $hashes;
    }

    public function getContent(string $hash):Array{
        $hashes = call_user_func_array([$this, 'getHashes'], $this->columns_name);
        
        if(isset($hashes[$hash])){
            return unserialize($hashes[$hash]);
        }

        return [];
    }
    
}   