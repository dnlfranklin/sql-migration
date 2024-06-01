<?php

namespace SqlMigration\Core;

class Differ{

    const CATALOG = [
        'SCHEMA',
        'SCHEMA CHARSET',
        'SCHEMA COLLATION',
        'DROP TABLE',
        'DROP VIEW',
        'DROP COLUMN',
        'DROP KEY',
        'DROP FOREIGN KEY',
        'TABLE',
        'COLUMN',
        'KEY',
        'FOREIGN KEY',
        'VIEW',
        'DATA',
    ];

    private $diffs = [];

    public function get(): Array{
        $list = [];

        foreach($this->diffs as $diff){
            if($diff['action']){
                $list[] = $diff['action'].' '.$diff['title'];
            }
        }

        return $list;
    }

    public function add(
        string $title, 
        string $catalog, 
        string $identifier,
        ?string $parent, 
        ?string $expected_value, 
        ?string $current_value
    ){
        if(!in_array($catalog, self::CATALOG)){
            throw new \Exception('Invalid catalog item');
        }
        
        $this->diffs[] = [
            'title' => $title,
            'catalog' => $catalog,
            'identifier' => $identifier,
            'parent' => $parent,
            'expected' => $expected_value,    
            'current' => $current_value,
            "action" => $expected_value == $current_value ? null : ($current_value ? 'UPDATE' : 'CREATE'),
        ];                
    }

    public function inject(Instruction $instruction):Instruction{
        foreach($this->diffs as $diff){
            if(!$diff['action']){
                continue;
            }

            $catalog    = $diff['catalog'];
            $identifier = $diff['identifier'];
            $parent     = $diff['parent'];
            $update     = $diff['action'] == 'UPDATE';
            
            switch($catalog){
                case 'SCHEMA':
                    $instruction->createSchema();
                    break;
                case 'SCHEMA CHARSET':
                    $instruction->alterSchemaCharset();
                    break;
                case 'SCHEMA COLLATION':
                    $instruction->alterSchemaCollation();
                    break;
                case 'DROP TABLE':
                    $instruction->dropTable($identifier);
                    break;
                case 'DROP VIEW':
                    $instruction->dropView($identifier);
                    break;
                case 'DROP COLUMN':
                    $instruction->dropColumn($parent, $identifier);
                    break;
                case 'DROP KEY':
                    $instruction->dropKey($parent, $identifier);
                    break;
                case 'DROP FOREIGN KEY':
                    $instruction->dropForeignKey($parent, $identifier);
                    break;
                case 'TABLE':
                    $update ? $instruction->alterTable($identifier) : $instruction->createTable($identifier);
                    break;
                case 'COLUMN':
                    $instruction->changeColumn($parent, $identifier, $update);
                    break;
                case 'KEY':
                    $instruction->changeKey($parent, $identifier, $update);
                    break;
                case 'FOREIGN KEY':
                    $instruction->changeForeignKey($parent, $identifier, $update);
                    break;
                case 'VIEW':
                    $instruction->createView($identifier);
                    break;
                case 'DATA':
                    $instruction->importData($parent, $identifier, $update);
                    break;
            }
        }

        return $instruction;
    }

}