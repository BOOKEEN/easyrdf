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
    /** Sparql 1.1 INSERT/DELETE quadData */
    const INSERT = 'INSERT';
    const DELETE = 'DELETE';

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
        if (\in_array($keyword, array(self::DELETE_DATA, self::DELETE_DATA_FROM, self::DELETE_FROM, self::DELETE,))) {
            $this->deleteKeyword = $keyword;
        } else {
            throw new Exception('Unknown delete keyword.');
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
        $query = $this->insertKeyword;
        switch ($this->insertKeyword) {
            case self::INSERT_DATA:
                $query .= $graphUri ? ' { GRAPH <' . $graphUri .'' : ' {';
                break;
            case self::INSERT:
                $query .= ' {';
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

        $query .= $this->formatRDFPayload($quadData);
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
        $query = $this->deleteKeyword;
        switch ($this->deleteKeyword) {
            case self::DELETE_DATA:
                $query .= $graphUri ? ' { GRAPH <' . $graphUri .'' : ' {';
                break;
            case self::DELETE:
                $query .= ' {';
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

        $query .= $this->formatRDFPayload($quadData);
        $query .= $graphUri ? '}}' : '}';

        return $this->update($query);
    }
}