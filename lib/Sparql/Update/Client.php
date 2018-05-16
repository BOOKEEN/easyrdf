<?php

namespace EasyRdf\Sparql\Update;

use EasyRdf\Exception;
use EasyRdf\Graph;
use EasyRdf\Http\Response as HttpResponse;
use EasyRdf\Sparql\Client as SparqlClient;
use EasyRdf\Sparql\Result;

class Client extends SparqlClient
{
    /** Sparql 1.1 compliant */
    /** Sparql 1.1 INSERT DATA quadData */
    const INSERT_DATA = 'INSERT DATA';
    /** Sparql 1.1 DELETE DATA quadData */
    const DELETE_DATA = 'DELETE DATA';
    /** Sparql 1.1 INSERT/DELETE quadPattern */
    const INSERT = 'INSERT';
    const DELETE = 'DELETE';
    const WITH = 'WITH';

    /** NOT Sparql 1.1 compliant */
    /** Virtuoso 6 */
    const INSERT_INTO = 'INSERT INTO';
    /** Virtuoso 6, 7 */
    const INSERT_IN = 'INSERT IN';
    const INSERT_IN_GRAPH = 'INSERT IN GRAPH';
    const DELETE_DATA_FROM = 'DELETE DATA FROM';
    const DELETE_FROM = 'DELETE FROM';


    /** @var string */
    private $usingGraphUri;

    /** @var string Made for some backward compatibility  */
    private $insertKeyword = self::INSERT_DATA;

    /** @var string Made for some backward compatibility  */
    private $deleteKeyword = self::DELETE_DATA;

    /** @var string Made for some backward compatibility  */
    private $deleteWhereKeyword = self::WITH;

    /**
     * @param string $sparqlEndpoint
     * @param string|null $usingGraphUri
     */
    public function __construct($sparqlEndpoint, $usingGraphUri = null)
    {
        parent::__construct($sparqlEndpoint);

        if ($usingGraphUri) {
            $this->usingGraphUri = $usingGraphUri;
        }
    }

    // Todo : Query Builder ?

    /**
     * Specify a using graph uri for an update Operation.
     * If using-graph-uri is provided. It will be used over USING or WITH update statement (when specified)
     *
     * Empty graph = remove
     *
     * @see https://www.w3.org/TR/2013/REC-sparql11-protocol-20130321/#update-dataset
     *
     * @param string $usingGraphUri
     */
    public function setUsingGraphUri($usingGraphUri)
    {
        $this->usingGraphUri = empty($usingGraphUri) ? null : $usingGraphUri;
    }

    /**
     * Keyword used while doing an insert data quadData.
     * Used for backward compatibility and issues for some triples store.
     *
     * Default value is sparql 1.1 compliant
     *
     * @param string $keyword
     *
     * @throws Exception Unknown Insert keyword
     */
    public function setInsertKeyword($keyword)
    {
        if (\in_array($keyword, array(
            self::INSERT_DATA,
            self::INSERT_IN,
            self::INSERT_IN_GRAPH,
            self::INSERT_INTO,
            self::INSERT,
        ))) {
            $this->insertKeyword = $keyword;
        } else {
            throw new Exception('Unknown Insert keyword.');
        }
    }

    /**
     * Keyword used while doing a delete data quadData.
     * Used for backward compatibility and issues for some triples store.
     *
     * Default value is sparql 1.1 compliant
     *
     * @param string $keyword
     *
     * @throws Exception Unknown delete keyword
     */
    public function setDeleteKeyword($keyword)
    {
        if (\in_array($keyword, array(self::DELETE_DATA, self::DELETE_DATA_FROM, self::DELETE_FROM, self::DELETE))) {
            $this->deleteKeyword = $keyword;
        } else {
            throw new Exception('Unknown delete keyword.');
        }
    }

    /**
     * Keyword used while doing a delete where quadPattern.
     * Used for backward compatibility and issues for some triples store.
     *
     * Default value is sparql 1.1 compliant
     *
     * @param string $keyword
     *
     * @throws Exception Unknown delete where keyword
     */
    public function setDeleteWhereKeyword($keyword)
    {
        if (\in_array($keyword, array(self::DELETE_FROM, self::WITH))) {
            $this->deleteWhereKeyword = $keyword;
        } else {
            throw new Exception('Unknown delete where keyword.');
        }
    }


    /**
     * @inheritdoc
     */
    public function update($query)
    {
        if ($this->usingGraphUri) {
            $this->addRdfDatasetParameter(sparqlClient::UPDATE_PARAM_USING_GRAPH, $this->usingGraphUri);
        }

        return parent::update($query);
    }

    /**
     * Insert a ground triple into the store
     *
     * @see https://www.w3.org/TR/sparql11-update/#insertData
     *
     * @param string $quadData RDF QuadData Payload
     * @param string|null $graphUri Specify a GRAPH to use against
     *
     * @return Graph|HttpResponse|Result
     *
     * @throws Exception Cannot use INSERT [in graph] without a graphUri
     */
    public function insertQuadData($quadData, $graphUri = null)
    {
        $ntripleList = $this->formatRDFPayload($quadData);
        $query = $this->insertKeyword;
        switch ($this->insertKeyword) {
            case self::INSERT_DATA:
                $query .= $graphUri ? ' { GRAPH <' . $graphUri .'' : ' {';
                break;
            case self::INSERT:
                $query .= ' {';
                if ($graphUri) {
                    $this->addRdfDatasetParameter(self::UPDATE_PARAM_USING_GRAPH, $graphUri);
                    // reset graphUri so we don't add two closing brackets
                    $graphUri = null;
                }
                break;
            case self::INSERT_IN:
            case self::INSERT_INTO:
            case self::INSERT_IN_GRAPH:
                if ($graphUri) {
                    $query .= ' <' . $graphUri . '> {';
                } else {
                    throw new Exception('Cannot use ' . $this->insertKeyword . ' without a graphUri');
                }
                break;
        }

        $query .= $ntripleList;
        $query .= $graphUri ? '}}' : '}';

        return $this->update($query);
    }

    /**
     * Delete a ground triple from the store
     *
     * @see https://www.w3.org/TR/sparql11-update/#deleteData
     *
     * @param string $quadData RDF QuadData Payload
     * @param string|null $graphUri Specify a GRAPH to use against
     *
     * @return Graph|HttpResponse|Result
     * @throws Exception Cannot use DELETE [in graph] without a graphUri
     */
    public function deleteQuadData($quadData, $graphUri = null)
    {
        $ntripleList = $this->formatRDFPayload($quadData);
        $query = $this->deleteKeyword;
        switch ($this->deleteKeyword) {
            case self::DELETE_DATA:
                $query .= $graphUri ? ' { GRAPH <' . $graphUri .'' : ' {';
                break;
            case self::DELETE:
                $query .= ' {';
                if ($graphUri) {
                    $this->addRdfDatasetParameter(self::UPDATE_PARAM_USING_GRAPH, $graphUri);
                    // reset graphUri so we don't add two closing brackets
                    $graphUri = null;
                }
                break;
            case self::DELETE_FROM:
            case self::DELETE_DATA_FROM:
                if ($graphUri) {
                    $query .= ' <' . $graphUri . '> {';
                } else {
                    throw new Exception('Cannot use ' . $this->deleteKeyword . ' without a graphUri');
                }
                break;
        }

        $query .= $ntripleList;
        $query .= $graphUri ? '}}' : '}';

        return $this->update($query);
    }

    /**
     * The DELETE WHERE operation is a shortcut form for the DELETE/INSERT operation
     * where bindings matched by the WHERE clause are used to define the triples in a graph that will be deleted.
     *
     * @see https://www.w3.org/TR/sparql11-update/#deleteWhere
     *
     * @param string $quadPattern RDF quadPattern Payload
     * @param string $graphUri Specify a GRAPH to use against
     *
     * @return Graph|HttpResponse|Result
     */
    public function deleteWhere($quadPattern, $graphUri)
    {
        switch ($this->deleteWhereKeyword) {
            case self::WITH:
                if ($graphUri) {
                    $query = self::WITH . ' <' . $graphUri . '> DELETE WHERE {' . $quadPattern . '}';
                } else {
                    $query = 'DELETE WHERE {' . $quadPattern . '}';
                }
                break;
            case self::DELETE_FROM:
                if ($graphUri) {
                    $query = self::DELETE_FROM . ' <' . $graphUri . '> {' . $quadPattern . '} WHERE {' . $quadPattern . '}';
                } else {
                    $query = 'DELETE {' . $quadPattern . '} WHERE {' . $quadPattern . '}';
                }
                break;
        }

        return $this->update($query);
    }
}