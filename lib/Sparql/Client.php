<?php
namespace EasyRdf\Sparql;

/**
 * EasyRdf
 *
 * LICENSE
 *
 * Copyright (c) 2009-2013 Nicholas J Humfrey.  All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 *    this list of conditions and the following disclaimer in the documentation
 *    and/or other materials provided with the distribution.
 * 3. The name of the author 'Nicholas J Humfrey" may be used to endorse or
 *    promote products derived from this software without specific prior
 *    written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 *
 * @package    EasyRdf
 * @copyright  Copyright (c) 2009-2013 Nicholas J Humfrey
 * @license    http://www.opensource.org/licenses/bsd-license.php
 */
use EasyRdf\Exception;
use EasyRdf\Format;
use EasyRdf\Graph;
use EasyRdf\Http;
use EasyRdf\Http\Client as HttpClient;
use EasyRdf\Http\Exception as HttpException;
use EasyRdf\Http\Response as HttpResponse;
use EasyRdf\RdfNamespace;
use EasyRdf\Utils;

/**
 * Class for making SPARQL queries using the SPARQL 1.1 Protocol
 *
 * @package    EasyRdf
 * @copyright  Copyright (c) 2009-2013 Nicholas J Humfrey
 * @license    http://www.opensource.org/licenses/bsd-license.php
 */
class Client
{
    /** application Content type */
    const CTYPE_SPARQL_UPDATE = 'application/sparql-update';
    const CTYPE_FORM_URLENCODED = 'application/x-www-form-urlencoded';

    /** The query/read address of the SPARQL Endpoint */
    private $queryUri = null;

    private $queryUriHasParam = false;

    /** The update/write address of the SPARQL Endpoint */
    private $updateUri = null;

    /** @var string */
    private $updateApplicationContentType = self::CTYPE_SPARQL_UPDATE;

    /** @var array */
    private $sparqlResultsTypes;

    /** Create a new SPARQL endpoint client
     *
     * If the query and update endpoints are the same, then you
     * only need to give a single URI.
     *
     * @param string $queryUri The address of the SPARQL Query Endpoint
     * @param string $updateUri Optional address of the SPARQL Update Endpoint
     */
    public function __construct($queryUri, $updateUri = null)
    {
        $this->queryUri = $queryUri;

        if (strlen(parse_url($queryUri, PHP_URL_QUERY)) > 0) {
            $this->queryUriHasParam = true;
        } else {
            $this->queryUriHasParam = false;
        }

        if ($updateUri) {
            $this->updateUri = $updateUri;
        } else {
            $this->updateUri = $queryUri;
        }

        // Tell the server which response formats we can parse
        // @see https://www.w3.org/TR/sparql11-protocol/#conneg
        $this->sparqlResultsTypes = array(
            'application/sparql-results+json' => 1.0,
            'application/sparql-results+xml' => 0.8
        );
    }

    /**
     * @param string $contentType  content type for update queries only. Can be sparql-update or x-www-form-urlencoded
     *               (@see https://www.w3.org/TR/2013/REC-sparql11-protocol-20130321/#update-operation)
     *
     * @throws Exception SPARQL UPDATE content type not valid
     */
    public function setUpdateApplicationContentType($contentType)
    {
        if (!in_array($contentType, array(self::CTYPE_SPARQL_UPDATE, self::CTYPE_FORM_URLENCODED))) {
            throw new Exception('SPARQL UPDATE content type not valid', 400);
        }

        $this->updateApplicationContentType = $contentType;
    }

    /** Get the URI of the SPARQL query endpoint
     *
     * @return string The query URI of the SPARQL endpoint
     */
    public function getQueryUri()
    {
        return $this->queryUri;
    }

    /** Get the URI of the SPARQL update endpoint
     *
     * @return string The query URI of the SPARQL endpoint
     */
    public function getUpdateUri()
    {
        return $this->updateUri;
    }

    /**
     * @depredated
     * @ignore
     */
    public function getUri()
    {
        return $this->queryUri;
    }

    /** Make a query to the SPARQL endpoint
     *
     * SELECT and ASK queries will return an object of type
     * EasyRdf\Sparql\Result.
     *
     * CONSTRUCT and DESCRIBE queries will return an object
     * of type EasyRdf\Graph.
     *
     * @param string $query The query string to be executed
     *
     * @return Result|\EasyRdf\Graph  Result of the query.
     */
    public function query($query)
    {
        return $this->request($query);
    }

    /** Count the number of triples in a SPARQL 1.1 endpoint
     *
     * Performs a SELECT query to estriblish the total number of triples.
     *
     * Counts total number of triples by default but a conditional triple pattern
     * can be given to count of a subset of all triples.
     *
     * @param string $condition Triple-pattern condition for the count query
     *
     * @return integer The number of triples
     */
    public function countTriples($condition = '?s ?p ?o')
    {
        // SELECT (COUNT(*) AS ?count)
        // WHERE {
        //   {?s ?p ?o}
        //   UNION
        //   {GRAPH ?g {?s ?p ?o}}
        // }
        $result = $this->query('SELECT (COUNT(*) AS ?count) {'.$condition.'}');
        return $result[0]->count->getValue();
    }

    /** Get a list of named graphs from a SPARQL 1.1 endpoint
     *
     * Performs a SELECT query to get a list of the named graphs
     *
     * @param string $limit Optional limit to the number of results
     *
     * @return \EasyRdf\Resource[]  array of objects for each named graph
     */
    public function listNamedGraphs($limit = null)
    {
        $query = "SELECT DISTINCT ?g WHERE {GRAPH ?g {?s ?p ?o}}";
        if (!is_null($limit)) {
            $query .= " LIMIT ".(int)$limit;
        }
        $result = $this->query($query);

        // Convert the result object into an array of resources
        $graphs = array();
        foreach ($result as $row) {
            array_push($graphs, $row->g);
        }
        return $graphs;
    }

    /** Make an update request to the SPARQL endpoint
     *
     * Successful responses will return the HTTP response object
     *
     * Unsuccessful responses will throw an exception
     *
     * @param string $query The update query string to be executed
     *
     * @return \EasyRdf\Http\Response HTTP response
     */
    public function update($query)
    {
        return $this->request($query);
    }

    /**
     * Prepare update data, make the update and return HTTP response (@see update)
     *
     * @param string|Graph $data data to update. Can be either a string or a Graph
     * @param string|null $graphUri graph uri to use while updating data
     * @param bool $oldSyntax true to use the old INSERT GRAPH syntax
     *
     * @return Http\Response
     */
    public function insert($data, $graphUri = null, $oldSyntax = false)
    {
        $query = 'INSERT DATA ';
        if ($graphUri) {
            $query .= $oldSyntax ? 'INTO <' . $graphUri . '> {' : '{ GRAPH <' . $graphUri . '> {';
        } else {
            $query .= '{';
        }
        $query .= $this->formatRDFPayload($data);
        if ($graphUri && !$oldSyntax) {
            $query .= '}';
        }
        $query .= '}';
        $updatedQuery = $this->addRdfNamespace($query);

        return $this->request($updatedQuery);
    }

    /**
     * Unused ?
     *
     * @param string $operation
     * @param string|Graph $data data to update. Can be either a string or a Graph
     * @param string|null $graphUri
     * @return Http\Response
     */
    protected function updateData($operation, $data, $graphUri = null)
    {
        $query = "$operation DATA {";
        if ($graphUri) {
            $query .= "GRAPH <$graphUri> {";
        }
        $query .= $this->formatRDFPayload($data);
        if ($graphUri) {
            $query .= "}";
        }
        $query .= '}';

        $updatedQuery = $this->addRdfNamespace($query);

        return $this->request($updatedQuery);
    }

    /**
     * This operation creates a graph in the Graph Store
     * @see https://www.w3.org/TR/2013/REC-sparql11-update-20130321/#create
     *
     * @param string $graphUri IRIref | DEFAULT | NAMES | ALL
     * @param bool $silent the result of the operation will always be success
     *
     * @return Http\Response
     */
    public function create($graphUri, $silent = false)
    {
        $query = $this->graphManagement(Graph::OPERATION_CREATE, $silent, $graphUri);

        return $this->request($query);
    }

    /**
     * The DROP operation removes the specified graph(s) from the Graph Store
     * @see https://www.w3.org/TR/sparql11-update/#drop
     *
     * @param string $graphUri IRIref | DEFAULT | NAMES | ALL
     * @param bool $silent the result of the operation will always be success
     *
     * @return Http\Response
     */
    public function drop($graphUri, $silent = false)
    {
        $query = $this->graphManagement(Graph::OPERATION_DROP, $silent, $graphUri);

        return $this->request($query);
    }

    /**
     * The CLEAR operation removes all the triples in the specified graph(s) in the Graph Store.
     * @see https://www.w3.org/TR/sparql11-update/#clear
     *
     * @param string $graphUri IRIref | DEFAULT | NAMES | ALL
     * @param bool $silent the result of the operation will always be success
     *
     * @return Http\Response
     */
    public function clear($graphUri, $silent = false)
    {
        $query = $this->graphManagement(Graph::UPDATE_CLEAR, $silent, $graphUri);

        return $this->request($query);
    }

    public function load($graphUriFrom, $graphUriTo, $silent = false)
    {
        $query = $this->graphManagement(Graph::UPDATE_LOAD, $silent, $graphUriFrom, $graphUriTo);

        return $this->request($query);
    }

    /**
     * The COPY operation is a shortcut for inserting all data from an input graph into a destination graph.
     * @see https://www.w3.org/TR/sparql11-update/#copy
     *
     * @param string $graphUriFrom IRIref | DEFAULT
     * @param string $graphUriTo IRIref | DEFAULT
     * @param bool $silent the result of the operation will always be success
     *
     * @return Http\Response
     */
    public function copy($graphUriFrom, $graphUriTo, $silent = false)
    {
        $query = $this->graphManagement(Graph::OPERATION_COPY, $silent, $graphUriFrom, $graphUriTo);

        return $this->request($query);
    }

    /**
     * The MOVE operation is a shortcut for moving all data from an input graph into a destination graph.
     * @see https://www.w3.org/TR/sparql11-update/#move
     *
     * @param string $graphUriFrom IRIref | DEFAULT
     * @param string $graphUriTo IRIref | DEFAULT
     * @param bool $silent the result of the operation will always be success
     *
     * @return Http\Response
     */
    public function move($graphUriFrom, $graphUriTo, $silent = false)
    {
        $query = $this->graphManagement(Graph::OPERATION_MOVE, $silent, $graphUriFrom, $graphUriTo);

        return $this->request($query);
    }

    /**
     * The ADD operation is a shortcut for inserting all data from an input graph into a destination graph
     * @see https://www.w3.org/TR/sparql11-update/#add
     *
     * @param string $graphUriFrom IRIref | DEFAULT
     * @param string $graphUriTo IRIref | DEFAULT
     * @param bool $silent the result of the operation will always be success
     *
     * @return Http\Response
     */
    public function add($graphUriFrom, $graphUriTo, $silent = false)
    {
        $query = $this->graphManagement(Graph::OPERATION_ADD, $silent, $graphUriFrom, $graphUriTo);

        return $this->request($query);
    }

    /**
     * Internal function to make an HTTP request to SPARQL endpoint
     *
     * @param string $query
     * @return Graph|HttpResponse|Result
     *
     * @throws HttpException HTTP request for SPARQL query failed
     */
    protected function request($query)
    {
        $response = $this->executeQuery($query, $uri, $callable, $checkQueryForm);

        if (!$response->isSuccessful()) {
            throw new HttpException(
                "HTTP request for SPARQL query failed",
                $response->getStatus(),
                null,
                $response->getBody()
            );
        }

        if ($response->getStatus() === 204) {
            // No content
            return $response;
        }

        return $this->parseResponseToQuery($response);
    }

    /**
     * Format the rdf payload for SPARQL Update operations
     *
     * @param string|Graph $data
     * @param string $format format to serialise data. List available : Format::getNames
     *
     * @return string The serialised graph
     * @throws Exception Error while trying to serialise
     * @throws Exception Data must be a string or a Graph
     */
    protected function formatRDFPayload($data, $format = 'ntriples')
    {
        if (is_string($data)) {

            return $data;
        } elseif (is_object($data) and $data instanceof Graph) {
            try {

                return $data->serialise($format);
            } catch (\Exception $e) {
                throw new Exception('Error while trying to serialise.', $e->getCode(), $e);
            }
        } else {
            throw new Exception('Cannot serialise. Data must be a string or a Graph.');
        }
    }

    /**
     * Adds missing prefix-definitions to the query
     *
     * Overriding classes may execute arbitrary query-alteration here
     *
     * @param string $query
     *
     * @return string rdfDataset using Prefixes
     */
    protected function addRdfNamespace($query)
    {
        // Check for undefined prefixes
        $prefixes = '';
        foreach (RdfNamespace::namespaces() as $prefix => $uri) {
            if (strpos($query, $prefix . ':') !== false &&
                strpos($query, 'PREFIX ' . $prefix . ':') === false
            ) {
                $prefixes .= 'PREFIX ' . $prefix . ': <' . $uri .'>' . PHP_EOL;
            }
        }

        return $prefixes . $query;
    }

    /**
     * @param string $query query to execute
     * @param string $uri The address of the SPARQL Query/Update Endpoint
     * @param Callable $callable Client Protocol to use
     * @param bool $checkQueryForm true to check query Form
     *
     * @return HttpResponse
     * @throws Exception User function not valid
     * @throws Exception Error while trying to call function
     */
    protected function executeQuery($query, $uri, $callable, $checkQueryForm)
    {
        $client = Http::getDefaultHttpClient();
        $client->resetParameters();

        if ($checkQueryForm) {
            $queryForm = $this->getQueryForm($query);
        } else {
            $queryForm = null;
        }
        $acceptHeader = $this->getHttpAcceptHeader($queryForm);
        $client->setHeaders('Accept', $acceptHeader);

        if (is_callable($callable)) {
            $client = call_user_func($callable, $query, $uri, $client);
        } else {
            throw new Exception('User function : ' . $callable . ' not valid.');
        }

        if (!$client) {
            throw new Exception('Error while trying to call : ' . $callable);
        }

        return $client->request();
    }

    /**
     * Prepare the client for a SPARQL GET Query
     * @see https://www.w3.org/TR/sparql11-protocol/#query-via-get
     *
     * Fallback to SPARQL POST Url-Encoded Query if we cannot use GET
     *
     * @param string $query query to execute
     * @param string $uri The address of the SPARQL Query Endpoint
     * @param HttpClient $client
     *
     * @return HttpClient $client
     */
    protected function sparqlGet($query, $uri, HttpClient $client)
    {
        $encodedQuery = 'query=' . urlencode($query);

        // 2046 = 2kB minus 1 for '?' and 1 for NULL-terminated string on server
        if (strlen($encodedQuery) + strlen($uri) <= 2046) {
            $delimiter = $this->queryUriHasParam ? '&' : '?';

            $client->setMethod('GET');
            $client->setUri($uri . $delimiter . $encodedQuery);

            return $client;
        } else {
            // Fall back to POST instead (which is un-cacheable)
            return $this->sparqlPostUrlEncoded($query, $uri, $client);
        }
    }

    /**
     * Prepare the client for a SPARQL POST Directly Query or Update
     *
     * @see https://www.w3.org/TR/sparql11-protocol/#query-via-post-direct
     * @see https://www.w3.org/TR/sparql11-protocol/#update-via-post-direct
     *
     * @param string $query query to execute
     * @param string $uri The address of the SPARQL Query/Update Endpoint
     * @param HttpClient $client
     *
     * @return HttpClient $client
     */
    protected function sparqlPostDirectly($query, $uri, HttpClient $client)
    {
        $client->setMethod('POST');
        $client->setUri($uri);
        $client->setRawData($query);
        $client->setHeaders('Content-Type', self::CTYPE_SPARQL_UPDATE);

        return $client;
    }

    /**
     * Prepare the client for a SPARQL POST Url-Encoded Query or Update
     *
     * @see https://www.w3.org/TR/sparql11-protocol/#query-via-post-urlencoded
     * @see https://www.w3.org/TR/sparql11-protocol/#update-via-post-urlencoded
     *
     * @param string $query query to execute
     * @param string $uri The address of the SPARQL Query/Update Endpoint
     * @param HttpClient $client
     *
     * @return HttpClient $client
     */
    protected function sparqlPostUrlEncoded($query, $uri, HttpClient $client)
    {
        $encodedQuery = 'query=' . urlencode($query);

        $client->setMethod('POST');
        $client->setUri($uri);
        $client->setRawData($encodedQuery);
        $client->setHeaders('Content-Type', self::CTYPE_FORM_URLENCODED);

        return $client;
    }

    /**
     * Get http Accept header.
     * @see https://www.w3.org/TR/2013/REC-sparql11-protocol-20130321/#conneg
     *
     *  - getHttpAcceptHeader : setup a list of default accept formats + extra formats if provided
     *  - formatAcceptHeader : define accept format
     *  - @see Format::getFormats() for a list of default formats.
     *
     * Success response format :
     * @see https://www.w3.org/TR/2013/REC-sparql11-protocol-20130321/#query-success
     * @see https://www.w3.org/TR/2013/REC-sparql11-protocol-20130321/#update-success
     *
     * @param string $queryForm
     * @return string
     */
    protected function getHttpAcceptHeader($queryForm = null)
    {
        if (!$queryForm) {

            return Format::getHttpAcceptHeader($this->sparqlResultsTypes);
        }

        switch ($queryForm) {
            case 'SELECT':
            case 'ASK':

                return Format::formatAcceptHeader($this->sparqlResultsTypes);
                break;
            case 'CONSTRUCT':
            case 'DESCRIBE':

                return Format::getHttpAcceptHeader();
                break;
            default:

                return Format::getHttpAcceptHeader($this->sparqlResultsTypes);
                break;
        }
    }

    /**
     * Retrieve a string representing a query form
     * @see https://www.w3.org/TR/sparql11-query/#QueryForms
     *
     * @param string $query query to execute
     * @return string|null QueryForm, null if it's a non standard query
     */
    protected function getQueryForm($query)
    {
        // regex for query forms
        $regex = '(?:(?:\s*BASE\s*<.*?>\s*)|(?:\s*PREFIX\s+.+:\s*<.*?>\s*))*'.
            '(CONSTRUCT|SELECT|ASK|DESCRIBE)[\W]';

        $result = null;
        $matched = mb_eregi($regex, $query, $result);

        if (false === $matched || count($result) !== 2) {
            // non-standard query. is this something non-standard?
            return 'UNKNOWN';
        } else {

            return strtoupper($result[1]);
        }
    }

    /**
     * Return a SPARQL query for graph management operations
     *
     * @param string $operation
     * @param bool $silent true to add SILENT statement
     * @param string $graphUriFrom graph Uri
     * @param string|null $graphUriTo graph Uri
     *
     * @return string SPARQL query
     */
    protected function graphManagement($operation, $silent, $graphUriFrom, $graphUriTo = null)
    {
        $query = $silent ? $operation . ' SILENT ' : $operation . ' ';

        switch ($operation) {
            case Graph::OPERATION_ADD:
            case Graph::OPERATION_COPY:
            case Graph::OPERATION_MOVE:
                $this->addGraphRef($query, $graphUriFrom, array(Graph::KEYWORD_DEFAULT));
                $query .= ' TO ';
                $this->addGraphRef($query, $graphUriTo, array(Graph::KEYWORD_DEFAULT));
                break;
            case Graph::OPERATION_DROP:
            case Graph::UPDATE_CLEAR:
                $this->addGraphRef(
                    $query,
                    $graphUriFrom,
                    array(Graph::KEYWORD_DEFAULT, Graph::KEYWORD_NAMED, Graph::KEYWORD_ALL)
                );
                break;
            case Graph::OPERATION_CREATE:
                $this->addGraphRef($query, $graphUriFrom, array());
                break;
            case Graph::UPDATE_LOAD:
                $this->addGraphRef($query, $graphUriFrom, array());
                $query .= ' INTO ';
                $this->addGraphRef($query, $graphUriTo, array());
                break;
            default:
                // do nothing
                break;
        }

        return $query;
    }

    /**
     * Update query with graph reference.
     * It may be a graph uri or a keyword (see GRAPH_* const)
     *
     * @param string $query initial query
     * @param string $graphUri graph(s) to update
     * @param string[] $graphList List of supported graph operation(s) (see GRAPH_* const)
     */
    protected function addGraphRef(&$query, $graphUri, array $graphList)
    {
        $graphMatch = implode('|', $graphList);
        if (preg_match('/^' . $graphMatch . '$/i', $graphUri)) {
            $query .= $graphUri;
        } else {
            $query .= 'GRAPH <' . $graphUri . '>';
        }
    }

    /**
     * Build http-client object, execute request and return a response
     *
     * @param string $processed_query
     * @param string $type            Should be either "query" or "update"
     *
     * @return Http\Response|\Zend\Http\Response
     * @throws Exception
     */
    protected function executeQuery2($processed_query, $type)
    {
        $client = Http::getDefaultHttpClient();
        $client->resetParameters();

        // Tell the server which response formats we can parse
        $sparql_results_types = array(
            'application/sparql-results+json' => 1.0,
            'application/sparql-results+xml' => 0.8
        );

        if ($type == 'update') {
            // accept anything, as "response body of a […] update request is implementation defined"
            // @see http://www.w3.org/TR/sparql11-protocol/#update-success
            $accept = Format::getHttpAcceptHeader($sparql_results_types);
            $client->setHeaders('Accept', $accept);

            if ($this->updateApplicationContentType === self::CTYPE_FORM_URLENCODED) {
                $encodedQuery = 'query=' . urlencode($processed_query);
                $client->setRawData($encodedQuery);
            } else {
                $client->setRawData($processed_query);
            }

            $client->setMethod('POST');
            $client->setUri($this->updateUri);
            $client->setHeaders('Content-Type', $this->updateApplicationContentType);
        } elseif ($type == 'query') {
            $re = '(?:(?:\s*BASE\s*<.*?>\s*)|(?:\s*PREFIX\s+.+:\s*<.*?>\s*))*'.
                '(CONSTRUCT|SELECT|ASK|DESCRIBE)[\W]';

            $result = null;
            $matched = mb_eregi($re, $processed_query, $result);

            if (false === $matched or count($result) !== 2) {
                // non-standard query. is this something non-standard?
                $query_verb = null;
            } else {
                $query_verb = strtoupper($result[1]);
            }

            if ($query_verb === 'SELECT' or $query_verb === 'ASK') {
                // only "results"
                $accept = Format::formatAcceptHeader($sparql_results_types);
            } elseif ($query_verb === 'CONSTRUCT' or $query_verb === 'DESCRIBE') {
                // only "graph"
                $accept = Format::getHttpAcceptHeader();
            } else {
                // both
                $accept = Format::getHttpAcceptHeader($sparql_results_types);
            }

            $client->setHeaders('Accept', $accept);

            $encodedQuery = 'query=' . urlencode($processed_query);

            // Use GET if the query is less than 2kB
            // 2046 = 2kB minus 1 for '?' and 1 for NULL-terminated string on server
            if (strlen($encodedQuery) + strlen($this->queryUri) <= 2046) {
                $delimiter = $this->queryUriHasParam ? '&' : '?';

                $client->setMethod('GET');
                $client->setUri($this->queryUri . $delimiter . $encodedQuery);
            } else {
                // Fall back to POST instead (which is un-cacheable)
                $client->setMethod('POST');
                $client->setUri($this->queryUri);
                $client->setRawData($encodedQuery);
                $client->setHeaders('Content-Type', self::CTYPE_FORM_URLENCODED);
            }
        } else {
            throw new Exception('unexpected request-type: '.$type);
        }

        return $client->request();
    }

    /**
     * Parse HTTP-response object into a meaningful result-object.
     * @see https://www.w3.org/TR/sparql11-protocol/#query-success
     *
     * Can be overridden to do custom processing
     *
     * @param Http\Response|\Zend\Http\Response $response
     * @return Graph|Result
     */
    protected function parseResponseToQuery($response)
    {
        list($contentType,) = Utils::parseMimeType($response->getHeader('Content-Type'));

        if (strpos($contentType, 'application/sparql-results') === 0) {
            $result = new Result($response->getBody(), $contentType);
            return $result;
        } else {
            $result = new Graph($this->queryUri, $response->getBody(), $contentType);
            return $result;
        }
    }
}
